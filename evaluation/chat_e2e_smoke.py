#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supersonic Chat E2E smoke runner (no external deps).

Goal:
- Send >=20 natural-language queries to a running Supersonic instance
- Force the time range to the last 3 years via `/api/chat/query/queryData`
- Fail fast on backend errors and on known SQL anti-patterns (metric/measure bizName leaks)

Usage (PowerShell):
  $env:SUPERSONIC_BASE_URL="http://127.0.0.1:9090"
  $env:SUPERSONIC_JWT_TOKEN="..."   # do NOT log this
  python -X utf8 evaluation/chat_e2e_smoke.py

Optional:
  python -X utf8 evaluation/chat_e2e_smoke.py --agent-id 5 --domain-id 13
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _today() -> dt.date:
    return dt.date.today()


def _years_ago(d: dt.date, years: int) -> dt.date:
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        # e.g. Feb 29 -> Feb 28
        return d.replace(year=d.year - years, day=28)


def _unwrap(resp: Any) -> Any:
    # Some endpoints return {code,msg,data}; some return raw list/object.
    if isinstance(resp, dict) and "data" in resp and "code" in resp:
        return resp["data"]
    return resp


@dataclass
class HttpClient:
    base_url: str
    token: str
    timeout_s: int = 180

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def get_json(self, path: str) -> Any:
        url = self.base_url + path
        req = urllib.request.Request(url, headers=self._headers(), method="GET")
        with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
            return json.load(resp)

    def post_json(self, path: str, payload: Any) -> Any:
        url = self.base_url + path
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=self._headers(), method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
            return json.load(resp)

    def post_form(self, path_with_query: str) -> Any:
        url = self.base_url + path_with_query
        req = urllib.request.Request(url, data=b"", headers=self._headers(), method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
            return json.load(resp)

def _meta(resp: Any) -> Dict[str, Any]:
    if not isinstance(resp, dict):
        return {"code": None, "msg": None, "traceId": None}
    return {"code": resp.get("code"), "msg": resp.get("msg"), "traceId": resp.get("traceId")}


def ensure_chat_id(client: HttpClient, agent_id: int, chat_name: str) -> int:
    chats = _unwrap(client.get_json(f"/api/chat/manage/getAll?agentId={agent_id}"))
    if isinstance(chats, dict):
        # Defensive: should be list, but tolerate unexpected wrapper
        chats = chats.get("list") or chats.get("records") or []
    for c in chats:
        if c.get("chatName") == chat_name and int(c.get("agentId") or agent_id) == agent_id:
            return int(c["chatId"])

    # create and re-fetch
    q = urllib.parse.urlencode({"chatName": chat_name, "agentId": agent_id})
    client.post_form(f"/api/chat/manage/save?{q}")
    chats = _unwrap(client.get_json(f"/api/chat/manage/getAll?agentId={agent_id}"))
    for c in chats:
        if c.get("chatName") == chat_name and int(c.get("agentId") or agent_id) == agent_id:
            return int(c["chatId"])
    raise RuntimeError(f"failed to create or find chatId for chatName={chat_name!r}")


def suspicious_sql_tokens(sql: str) -> List[str]:
    # Heuristic: physical SQL must not reference metric/measure bizName like
    # `fact_internet_sales_sales_amount` as a column.
    # Table names use `__` (double underscore) in this setup, so `fact_*_*_*` is a good signal.
    if not sql:
        return []
    tokens = []
    for word in sql.replace("\n", " ").replace("\t", " ").split():
        w = word.strip(",;()")
        if w.startswith('"') and w.endswith('"') and len(w) > 2:
            w = w[1:-1]
        if w.startswith("fact_") and w.count("_") >= 3 and "__" not in w:
            tokens.append(w)
    # de-dup while keeping order
    seen = set()
    out = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def build_queries() -> List[str]:
    # >= 20 queries covering multiple subject areas.
    return [
        # internet sales
        "每个产品键的总销售金额（最近三年）",
        "按订单日期（月）统计总销售金额趋势（最近三年）",
        "按销售区域组统计总销售金额（最近三年）",
        "按销售国家统计总销售金额 Top10（最近三年）",
        "每个促销活动的总折扣金额（最近三年）",
        "平均折扣率按产品键 Top10（最近三年）",
        "最低单价和最高单价分别是多少（最近三年，按产品键）",
        "总运费按销售区域排序（最近三年）",
        "总税费按订单日期（月）趋势（最近三年）",
        "总订单数量按产品键 Top10（最近三年）",
        # reseller sales
        "经销商销售总额按产品键 Top10（最近三年）",
        "经销商订单数量总和按销售区域键（最近三年）",
        "经销商折扣金额总和按订单日期（月）趋势（最近三年）",
        "经销商平均单价按产品键 Top10（最近三年）",
        # inventory
        "产品库存：总出库数量按Movement日期（月）趋势（最近三年）",
        "产品库存：总入库数量按产品键 Top10（最近三年）",
        "产品库存：总库存价值按产品键 Top10（最近三年）",
        # finance
        "财务：总财务金额按Finance日期（月）趋势（最近三年）",
        "财务：总收入金额和总支出金额对比（最近三年，按Finance日期（月））",
        # call center
        "呼叫中心：总呼叫数按Call日期（月）趋势（最近三年）",
        "呼叫中心：呼叫产生订单数按Shift（最近三年）",
        # currency rate
        "汇率：平均汇率按Rate日期（月）趋势（最近三年）",
        "汇率：平均收盘汇率按货币键（最近三年）",
        # sales quota
        "销售配额：总销售配额按Quota日期（月）趋势（最近三年）",
        "销售配额：人均销售配额按日历年（最近三年）",
        # survey
        "调查：总调查响应数按调查日期（月）趋势（最近三年）",
        "调查：参与调查客户数按英文产品类别名称 Top10（最近三年）",
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=os.environ.get("SUPERSONIC_BASE_URL", "http://127.0.0.1:9090"))
    parser.add_argument("--token", default=os.environ.get("SUPERSONIC_JWT_TOKEN", ""))
    parser.add_argument("--agent-id", type=int, default=5)
    parser.add_argument("--timeout-s", type=int, default=180)
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    token = (args.token or "").strip()
    if not token:
        print("ERROR: missing SUPERSONIC_JWT_TOKEN (set env var; do not print it).", file=sys.stderr)
        return 2

    client = HttpClient(base_url=base_url, token=token, timeout_s=args.timeout_s)

    run_id = time.strftime("%Y%m%d_%H%M%S")
    chat_name = f"codex_chat_e2e_{run_id}"
    chat_id = ensure_chat_id(client, args.agent_id, chat_name)

    end = _today()
    start = _years_ago(end, 3)
    date_info = {
        "dateMode": "BETWEEN",
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "period": "DAY",
        "unit": 1,
    }

    queries = build_queries()
    results: List[Dict[str, Any]] = []

    print(f"Supersonic={base_url}, agentId={args.agent_id}, chatId={chat_id}, date={start}..{end}")
    print(f"Queries={len(queries)} (expected >= 20)")

    for idx, q in enumerate(queries, start=1):
        item: Dict[str, Any] = {"idx": idx, "queryText": q}
        t0 = time.time()
        try:
            parse_payload = {
                "queryText": q,
                "agentId": args.agent_id,
                "chatId": chat_id,
                "saveAnswer": False,
                "disableLLM": False,
            }
            parse_resp = client.post_json("/api/chat/query/parse", parse_payload)
            parse_data = _unwrap(parse_resp)
            item["parse"] = _meta(parse_resp)
            if item["parse"]["code"] not in (None, 200) or not isinstance(parse_data, dict):
                item["ok"] = False
                item["error"] = f"parse failed: {item['parse']}"
                results.append(item)
                print(f"[{idx:02d}] FAIL parse: {q} | {item['parse']}")
                continue

            item["queryId"] = parse_data.get("queryId")
            parses = parse_data.get("selectedParses") or []
            if not parses:
                item["ok"] = False
                item["error"] = "no selectedParses"
                results.append(item)
                print(f"[{idx:02d}] FAIL parse: {q}")
                continue
            parse_id = parses[0].get("id")
            item["parseId"] = parse_id

            qd_payload = {"queryId": item["queryId"], "parseId": parse_id, "dateInfo": date_info}
            qd_resp = client.post_json("/api/chat/query/queryData", qd_payload)
            qd_data = _unwrap(qd_resp)
            item["query"] = _meta(qd_resp)
            if item["query"]["code"] not in (None, 200) or not isinstance(qd_data, dict):
                item["ok"] = False
                item["error"] = f"queryData failed: {item['query']}"
                results.append(item)
                print(f"[{idx:02d}] FAIL queryData: {q} | {item['query']}")
                continue

            query_state = qd_data.get("queryState")
            item["query"]["queryState"] = query_state
            item["query"]["errorMsg"] = qd_data.get("errorMsg")

            sql = qd_data.get("querySql") or ""
            item["query"]["querySqlHead"] = sql[:260].replace("\n", " ")

            bad_tokens = suspicious_sql_tokens(sql)
            if bad_tokens:
                item["query"]["suspiciousSqlTokens"] = bad_tokens[:20]
                item["ok"] = False
                item["error"] = f"suspicious SQL tokens: {bad_tokens[:5]}"
                results.append(item)
                print(f"[{idx:02d}] FAIL sql: {q} | tokens={bad_tokens[:3]}")
                continue

            if str(query_state).upper() != "SUCCESS":
                item["ok"] = False
                item["error"] = f"queryState={query_state}, errorMsg={qd_data.get('errorMsg')}"
                results.append(item)
                print(f"[{idx:02d}] FAIL query: {q} | state={query_state}")
                continue

            item["ok"] = True
            item["latencyMs"] = int((time.time() - t0) * 1000)
            results.append(item)
            print(f"[{idx:02d}] OK   ({item['latencyMs']}ms) {q}")
        except Exception as e:  # noqa: BLE001
            item["ok"] = False
            item["error"] = f"{type(e).__name__}: {e}"
            results.append(item)
            print(f"[{idx:02d}] ERROR {q} | {type(e).__name__}: {e}")

    ok_cnt = sum(1 for r in results if r.get("ok"))
    fail_cnt = len(results) - ok_cnt
    print(f"Summary: ok={ok_cnt}, fail={fail_cnt}, total={len(results)}")

    out_path = args.out.strip()
    if not out_path:
        out_dir = os.path.join(os.path.dirname(__file__), "..", "output")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.abspath(os.path.join(out_dir, f"chat_e2e_smoke_{run_id}.json"))

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "baseUrl": base_url,
                "agentId": args.agent_id,
                "chatId": chat_id,
                "date": {"start": start.isoformat(), "end": end.isoformat()},
                "results": results,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"Report: {out_path}")

    return 0 if fail_cnt == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
