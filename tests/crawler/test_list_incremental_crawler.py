from __future__ import annotations

import json
import textwrap
from pathlib import Path

import list_incremental_crawler as lic
import pytest
from tests.crawler.conftest import _AsyncWebCrawler, _FakeResult
from list_incremental_crawler import ListIncrementalCrawler


@pytest.fixture
def crawler(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ListIncrementalCrawler:
    monkeypatch.setattr(lic, "AsyncWebCrawler", _AsyncWebCrawler)
    state_file = tmp_path / "state" / "seen.json"
    instance = ListIncrementalCrawler(
        config_dir=str(tmp_path / "config_data"),
        cache_base_directory=str(tmp_path / "cache"),
        state_file=str(state_file),
    )
    return instance


def test_build_list_page_url(crawler: ListIncrementalCrawler) -> None:
    assert crawler._build_list_page_url("https://a.com/list.htm", 1) == "https://a.com/list.htm"
    assert crawler._build_list_page_url("https://a.com/list.htm", 2) == "https://a.com/list2.htm"
    assert crawler._build_list_page_url("https://a.com/list3.htm", 4) == "https://a.com/list4.htm"


def test_is_allowed_and_normalize(crawler: ListIncrementalCrawler) -> None:
    assert (
        crawler._normalize_link("/a/123.htm", "jwc.seu.edu.cn")
        == "https://jwc.seu.edu.cn/a/123.htm"
    )
    assert crawler._normalize_link("javascript:void(0)", "jwc.seu.edu.cn") is None
    assert crawler._is_allowed(
        "https://jwc.seu.edu.cn/a/123.htm",
        [r"/a/\d+\.htm$"],
        [r"/list\d*\.htm$"],
    )
    assert not crawler._is_allowed(
        "https://jwc.seu.edu.cn/list2.htm",
        [r"jwc\.seu\.edu\.cn"],
        [r"/list\d*\.htm$"],
    )


def test_state_file_roundtrip(crawler: ListIncrementalCrawler, tmp_path: Path) -> None:
    state_file = tmp_path / "state" / "a.json"
    crawler._save_state({"u1", "u2"}, state_file)
    loaded = crawler._load_state(state_file)
    assert loaded == {"u1", "u2"}


@pytest.mark.asyncio
async def test_crawl_list_incremental_dedup_and_state(
    crawler: ListIncrementalCrawler, tmp_path: Path
) -> None:
    cfg_dir = tmp_path / "config_data"
    (cfg_dir / "websites").mkdir(parents=True, exist_ok=True)

    _AsyncWebCrawler.results_by_url = {
        "https://jwc.seu.edu.cn/jwxx/list.htm": _FakeResult(
            True,
            {
                "internal": [
                    {"href": "/jwxx/1001.htm"},
                    {"href": "/jwxx/list2.htm"},
                    {"href": "https://jwc.seu.edu.cn/zxdt/2001.htm"},
                ]
            },
        ),
        "https://jwc.seu.edu.cn/jwxx/list2.htm": _FakeResult(False),
    }

    first = await crawler.crawl_list_incremental(
        list_url="https://jwc.seu.edu.cn/jwxx/list.htm",
        max_pages=3,
        include_patterns=[r"jwc\.seu\.edu\.cn/.+/\d+\.htm$"],
        exclude_patterns=[r"/list\d*\.htm$"],
    )
    assert sorted(first) == [
        "https://jwc.seu.edu.cn/jwxx/1001.htm",
        "https://jwc.seu.edu.cn/zxdt/2001.htm",
    ]

    second = await crawler.crawl_list_incremental(
        list_url="https://jwc.seu.edu.cn/jwxx/list.htm",
        max_pages=3,
        include_patterns=[r"jwc\.seu\.edu\.cn/.+/\d+\.htm$"],
        exclude_patterns=[r"/list\d*\.htm$"],
    )
    assert second == []


@pytest.mark.asyncio
async def test_crawl_website_incremental_with_two_start_urls_smoke(
    crawler: ListIncrementalCrawler, tmp_path: Path
) -> None:
    cfg_dir = tmp_path / "config_data"
    websites_dir = cfg_dir / "websites"
    websites_dir.mkdir(parents=True, exist_ok=True)
    state_file = (tmp_path / "tmp" / "jwc_seen_urls.json").as_posix()
    cache_dir = (tmp_path / "tmp" / "crawl4ai_jwc_cache").as_posix()

    (cfg_dir / "browser.yaml").write_text("browser: {}\n", encoding="utf-8")
    (cfg_dir / "crawler.yaml").write_text("crawler: {}\n", encoding="utf-8")

    (websites_dir / "jwc.yaml").write_text(
        textwrap.dedent(
            f"""
            website:
              name: "SEU JWC"
              base_url: "https://jwc.seu.edu.cn"
              start_urls:
                - "https://jwc.seu.edu.cn/jwxx/list.htm"
                - "https://jwc.seu.edu.cn/zxdt/list.htm"
              list_incremental:
                enabled: true
                max_pages: 31
                state_file: "{state_file}"
                cache_base_directory: "{cache_dir}"
                include_patterns:
                                    - 'jwc\\.seu\\.edu\\.cn/.+/\\d+\\.htm$'
                exclude_patterns:
                                    - '/list\\d*\\.htm$'
                                    - 'javascript:'
              overrides:
                crawler:
                  cache_mode: "ENABLED"
                  check_cache_freshness: true
                browser:
                  headless: true
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    _AsyncWebCrawler.results_by_url = {
        "https://jwc.seu.edu.cn/jwxx/list.htm": _FakeResult(
            True,
            {
                "internal": [
                    {"href": "/jwxx/1001.htm"},
                    {"href": "/common/9999.htm"},
                ]
            },
        ),
        "https://jwc.seu.edu.cn/zxdt/list.htm": _FakeResult(
            True,
            {
                "internal": [
                    {"href": "/zxdt/2001.htm"},
                    {"href": "/common/9999.htm"},
                ]
            },
        ),
        "https://jwc.seu.edu.cn/jwxx/list2.htm": _FakeResult(False),
        "https://jwc.seu.edu.cn/zxdt/list2.htm": _FakeResult(False),
    }

    result = await crawler.crawl_website_incremental("jwc")

    assert result["website"] == "jwc"
    assert result["start_urls"] == [
        "https://jwc.seu.edu.cn/jwxx/list.htm",
        "https://jwc.seu.edu.cn/zxdt/list.htm",
    ]
    assert len(result["lists"]) == 2
    assert sorted(result["incremental_urls"]) == [
        "https://jwc.seu.edu.cn/common/9999.htm",
        "https://jwc.seu.edu.cn/jwxx/1001.htm",
        "https://jwc.seu.edu.cn/zxdt/2001.htm",
    ]

    list_state_files = [Path(item["state_file"]) for item in result["lists"]]
    assert list_state_files[0] != list_state_files[1]
    assert all(path.exists() for path in list_state_files)

    first_state_entries = json.loads(list_state_files[0].read_text(encoding="utf-8"))
    second_state_entries = json.loads(list_state_files[1].read_text(encoding="utf-8"))
    assert isinstance(first_state_entries, list)
    assert isinstance(second_state_entries, list)
