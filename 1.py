import csv
import os
import re
import time
import traceback
import threading
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import shutil

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ----------------- 路径与参数 -----------------
# 请确保这个路径正确无误，并且文件存在
INPUT_GOIDS_CSV = r"title.csv"
OUTPUT_RESULT_CSV = r"results.csv"

ADVANCED_URL = "https://www.proquest.com/advanced"
TIMEOUT = 30

# 注意：请根据你的运行阶段修改这两个参数
HEADLESS = False  # 首次登录请改为 False
MAX_WORKERS = 3  # 首次登录请改为 1

DELAY_RANGE = (2, 5)  # 随机延迟范围（秒）

# 为每个线程创建独立的用户目录
BASE_PROFILE_DIR = str(Path.home() / "proquest_profiles")

# 线程安全的锁
file_lock = threading.Lock()
driver_creation_lock = threading.Lock()


# ------------------------------------------------

def ensure_parent(path_like: str):
    parent = os.path.dirname(os.path.abspath(path_like)) or "."
    Path(parent).mkdir(parents=True, exist_ok=True)


def setup_driver(worker_id: int, headless: bool = True) -> webdriver.Chrome:
    """为每个工作线程创建独立的Chrome实例，并禁用图片加载"""
    with driver_creation_lock:
        opts = Options()
        if headless:
            opts.add_argument("--headless=new")

        # 禁用图片加载以提高速度
        opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

        opts.add_argument("--window-size=1600,1000")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-popup-blocking")
        opts.add_argument("--disable-web-security")
        opts.add_argument("--disable-features=VizDisplayCompositor")

        # 每个worker使用独立的用户目录
        profile_dir = f"{BASE_PROFILE_DIR}/worker_{worker_id}"
        opts.add_argument(f"--user-data-dir={profile_dir}")
        opts.add_argument("--profile-directory=Default")

        # 随机化User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]
        opts.add_argument(f"--user-agent={random.choice(user_agents)}")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)

        # 设置隐式等待
        driver.implicitly_wait(10)

        # 随机延迟，避免同时启动
        time.sleep(random.uniform(1, 3))

        return driver


def wait_ready(drv: webdriver.Chrome, timeout: int = TIMEOUT):
    try:
        WebDriverWait(drv, timeout).until(
            lambda x: x.execute_script("return document.readyState") == "complete"
        )
        time.sleep(random.uniform(0.5, 1.0))
    except Exception:
        time.sleep(1)


def read_goids(csv_path: str) -> List[str]:
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return []

    has_header = any("goid" in str(c).strip().lower() for c in rows[0])
    start_row = 1 if has_header else 0

    out, seen = [], set()
    for row in rows[start_row:]:
        if not row:
            continue
        goid_str = row[0] if row else ""
        g = re.sub(r"\D", "", goid_str)
        if g and g not in seen:
            out.append(g)
            seen.add(g)
    return out


def handle_popups(driver: webdriver.Chrome):
    try:
        for xpath in [
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'accept')]",
            "//button[contains(., '同意')]",
            "//button[contains(., '同意所有')]",
        ]:
            els = driver.find_elements(By.XPATH, xpath)
            for e in els:
                if e.is_displayed():
                    e.click()
                    time.sleep(0.5)
        for sel in [".modal .close", ".modal button.close", ".btn-close"]:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for e in els:
                if e.is_displayed():
                    e.click()
                    time.sleep(0.3)
    except Exception:
        pass


def on_advanced_search_page(driver: webdriver.Chrome) -> bool:
    candidates = [
        (By.ID, "queryTermField"),
        (By.NAME, "queryTermField"),
        (By.CSS_SELECTOR, "input#queryTermField, input[name='queryTermField']"),
    ]
    for by, val in candidates:
        try:
            els = driver.find_elements(by, val)
            if any(e.is_displayed() for e in els):
                return True
        except Exception:
            continue
    return False


def ensure_advanced_page(driver: webdriver.Chrome, worker_id: int):
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Worker-{worker_id}: 尝试进入高级检索页面 (第{attempt}次)")
            driver.get(ADVANCED_URL)
            wait_ready(driver)
            handle_popups(driver)

            if on_advanced_search_page(driver):
                print(f"Worker-{worker_id}: 成功进入高级检索页面")
                return

            page = driver.page_source
            if re.search(r"Sign in|Login|登录|Institution|Shibboleth|机构", page, flags=re.I):
                print(f"Worker-{worker_id}: 检测到登录页面，无法自动处理。请在有头模式下手动登录。")
                if not HEADLESS:
                    print("请在打开的浏览器中完成登录，然后手动关闭浏览器或按Ctrl+C停止脚本。")
                    time.sleep(900)
                    return
                else:
                    raise RuntimeError(f"Worker-{worker_id}: 登录状态已过期或未保存，无法继续。")

            time.sleep(random.uniform(1, 2))
        except Exception as e:
            print(f"Worker-{worker_id}: 进入高级检索页面失败: {e}")
            if attempt == max_retries:
                raise RuntimeError(f"Worker-{worker_id}: 多次尝试仍未进入高级检索页面")
            time.sleep(random.uniform(2, 4))


def extract_first_result_info(page_source: str) -> Optional[Dict[str, str]]:
    soup = BeautifulSoup(page_source, "html.parser")
    link = soup.select_one("h3 a, h2 a, a[href*='docview']")
    if not link:
        return None
    href = link.get("href", "").strip()
    if href.startswith("/"):
        href = "https://www.proquest.com" + href
    title = link.get_text(strip=True)

    date, pub = "", ""
    container = link.find_parent()
    if container:
        dnode = container.select_one(".publication-date, .date, [data-testid='publication-date']")
        if dnode: date = dnode.get_text(strip=True)
        pnode = container.select_one(".publication-title, .source, .pubTitle, .pubtitle")
        if pnode: pub = pnode.get_text(strip=True)

    return {"title": title, "link": href, "date": date, "publication": pub}


def extract_article_body(driver: webdriver.Chrome, url: str, timeout: int = TIMEOUT) -> str:
    try:
        orig = driver.current_window_handle
        driver.execute_script("window.open(arguments[0],'_blank');", url)
        driver.switch_to.window(driver.window_handles[-1])
        wait_ready(driver, timeout)

        selectors = [
            ".full-text", ".document-content", ".article-body",
            ".article-content", ".story-body", "div[data-testid='article-body']"
        ]
        body = ""
        for sel in selectors:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                body = els[0].text.strip()
                break
        if not body:
            ps = driver.find_elements(By.TAG_NAME, "p")
            paras = [p.text.strip() for p in ps if p.text and len(p.text.strip()) > 20]
            if paras:
                body = "\n".join(paras)

        driver.close()
        driver.switch_to.window(orig)
        return body
    except Exception as e:
        print(f"提取文章内容失败: {e}")
        try:
            driver.switch_to.window(orig)
        except:
            pass
        return ""


def write_result_to_csv(result_data: Dict, writer: csv.DictWriter):
    """线程安全地写入CSV"""
    with file_lock:
        writer.writerow(result_data)


def process_single_goid(driver: webdriver.Chrome, goid: str, worker_id: int, writer: csv.DictWriter) -> Dict:
    """处理单个GOID的完整流程，不重复加载页面"""
    try:
        print(f"Worker-{worker_id}: 开始处理 GOID {goid}")

        # 找到搜索框并输入GOID
        box = None
        for by, val in [
            (By.ID, "queryTermField"),
            (By.NAME, "queryTermField"),
            (By.CSS_SELECTOR, "input#queryTermField, input[name='queryTermField']"),
        ]:
            try:
                box = WebDriverWait(driver, TIMEOUT).until(
                    EC.element_to_be_clickable((by, val))
                )
                if box:
                    break
            except Exception:
                continue

        if not box:
            raise RuntimeError("未找到搜索框")

        box.clear()
        box.send_keys(goid)

        # 找到搜索按钮并点击
        btn = None
        for by, val in [
            (By.ID, "searchToResultPage"),
            (By.CSS_SELECTOR, "button#searchToResultPage, button[type='submit']"),
        ]:
            try:
                btn = WebDriverWait(driver, TIMEOUT).until(
                    EC.element_to_be_clickable((by, val))
                )
                if btn:
                    break
            except Exception:
                continue

        if not btn:
            raise RuntimeError("未找到搜索按钮")

        btn.click()
        wait_ready(driver, TIMEOUT)
        handle_popups(driver)

        src = driver.page_source
        if re.search(r"No results|没有找到|0 results", src, flags=re.I):
            print(f"Worker-{worker_id}: GOID {goid} 无搜索结果")
            result = dict(
                goid=goid, matched_title="", date="", publication="", url="",
                content="", content_length=0
            )
            write_result_to_csv(result, writer)
            return result

        info = extract_first_result_info(src)
        if not info:
            print(f"Worker-{worker_id}: GOID {goid} 无法解析结果")
            result = dict(
                goid=goid, matched_title="", date="", publication="", url="",
                content="", content_length=0
            )
            write_result_to_csv(result, writer)
            return result

        body = extract_article_body(driver, info["link"], TIMEOUT)

        result = dict(
            goid=goid,
            matched_title=info.get("title", ""),
            date=info.get("date", ""),
            publication=info.get("publication", ""),
            url=info.get("link", ""),
            content=body or "",
            content_length=len(body or "")
        )

        write_result_to_csv(result, writer)
        print(f"Worker-{worker_id}: 成功处理 GOID {goid} (正文 {len(body or '')} 字符)")

        return result

    except Exception as e:
        print(f"Worker-{worker_id}: 处理 GOID {goid} 失败: {e}")
        traceback.print_exc()

        result = dict(
            goid=goid, matched_title="ERROR", date="", publication="", url="",
            content=str(e), content_length=0
        )
        write_result_to_csv(result, writer)
        return result


def worker_function(goid_batch: List[str], worker_id: int, writer: csv.DictWriter):
    """工作线程函数，每个线程只初始化一次浏览器实例并登录"""
    driver = None
    try:
        print(f"Worker-{worker_id}: 正在初始化浏览器...")
        driver = setup_driver(worker_id, HEADLESS)

        # 仅在线程启动时检查登录状态并尝试进入高级检索页面
        ensure_advanced_page(driver, worker_id)

        print(f"Worker-{worker_id}: 初始化完成，开始处理 {len(goid_batch)} 个GOIDs")

        for i, goid in enumerate(goid_batch, 1):
            try:
                print(f"Worker-{worker_id}: [{i}/{len(goid_batch)}] 处理 {goid}")
                # 在同一个实例上进行后续搜索，不再重复加载页面
                process_single_goid(driver, goid, worker_id, writer)

                if i < len(goid_batch):
                    delay = random.uniform(*DELAY_RANGE)
                    time.sleep(delay)

            except Exception as e:
                print(f"Worker-{worker_id}: 处理 {goid} 时出现异常: {e}")
                continue

    except Exception as e:
        print(f"Worker-{worker_id}: 线程初始化失败: {e}")
        traceback.print_exc()

    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                print(f"Worker-{worker_id}: 关闭driver失败: {e}")

    print(f"Worker-{worker_id}: 完成所有任务")


def split_goids_for_workers(goids: List[str], num_workers: int) -> List[List[str]]:
    """将GOID列表分配给各个工作线程"""
    chunk_size = (len(goids) + num_workers - 1) // num_workers
    chunks = [goids[i:i + chunk_size] for i in range(0, len(goids), chunk_size)]
    return [chunk for chunk in chunks if chunk]


def main():
    print("开始运行 ProQuest 爬虫...")

    if not os.path.exists(INPUT_GOIDS_CSV):
        print(f"找不到输入 CSV：{INPUT_GOIDS_CSV}")
        return

    Path(BASE_PROFILE_DIR).mkdir(parents=True, exist_ok=True)
    ensure_parent(OUTPUT_RESULT_CSV)

    goids = read_goids(INPUT_GOIDS_CSV)
    if not goids:
        print("输入 CSV 中没有有效 GOID")
        return

    print(f"读取到 {len(goids)} 个 GOID，将使用 {MAX_WORKERS} 个并行线程")

    new_file = not os.path.exists(OUTPUT_RESULT_CSV)
    f_out = open(OUTPUT_RESULT_CSV, "a", encoding="utf-8-sig", newline="")
    writer = csv.DictWriter(
        f_out,
        fieldnames=["goid", "matched_title", "date", "publication", "url", "content", "content_length"]
    )
    if new_file:
        writer.writeheader()

    try:
        start_time = time.time()

        goid_chunks = split_goids_for_workers(goids, MAX_WORKERS)
        print(f"任务分配: {[len(chunk) for chunk in goid_chunks]}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for worker_id, goid_batch in enumerate(goid_chunks):
                future = executor.submit(worker_function, goid_batch, worker_id + 1, writer)
                futures.append(future)

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Worker 任务执行失败: {e}")

        end_time = time.time()
        elapsed = end_time - start_time

        print(f"\n完成！总用时: {elapsed:.2f} 秒")
        print(f"平均每个GOID用时: {elapsed / len(goids):.2f} 秒")
        print(f"输出文件：{OUTPUT_RESULT_CSV}")

    finally:
        try:
            f_out.close()
        except Exception:
            pass


if __name__ == "__main__":
    print("""
    ======================
    重要操作提示：
    ======================
    首次运行（仅需执行一次）：
    - 确保 HEADLESS = False 和 MAX_WORKERS = 1
    - 运行脚本，手动完成登录，然后让脚本正常退出或手动停止。
    - 登录会话将自动保存在： /Users/jinanwuyanzu/proquest_profiles

    后续运行（正常爬取）：
    - 确保 HEADLESS = True 和 MAX_WORKERS = 你需要的线程数
    - 运行脚本，即可开始无头并行爬取。
    """)

    main()
