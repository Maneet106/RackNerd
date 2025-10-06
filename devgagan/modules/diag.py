import time
from typing import List
from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime
import pytz

from devgagan import app
from config import OWNER_ID
from devgagan.core.session_pool import session_pool
from devgagan.core.metrics import metrics
from devgagan.core.mongo.plans_db import check_premium


def _fmt_ts(ts: float) -> str:
    """Format a POSIX timestamp in Asia/Kolkata timezone (UTC+05:30)."""
    try:
        ist = pytz.timezone("Asia/Kolkata")
        # Guard against None/0
        if not ts:
            return "-"
        return datetime.fromtimestamp(float(ts), ist).strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return "-"


@app.on_message(filters.command(["diag", "pool"]) & filters.private)
async def diag_pool(_, message):
    user_id = message.from_user.id if message.from_user else message.chat.id
    # Silent admin check - no response for non-admins
    if user_id not in OWNER_ID:
        return

    # Session pool diagnostics
    diag = await session_pool.get_diagnostics()
    waiters = diag.pop("_waiters", {"premium": 0, "free": 0})

    # Metrics snapshot
    snap = await metrics.snapshot()
    tasks = snap.get("tasks", [])
    per_session = snap.get("per_session", {})

    # Compute premium/free counts for current tasks
    prem_count = 0
    free_count = 0
    for t in tasks:
        try:
            doc = await check_premium(int(t.get("user_id")))
            if doc:
                prem_count += 1
            else:
                free_count += 1
        except Exception:
            free_count += 1

    # Build response (HTML)
    lines: List[str] = []
    lines.append("<b>🧭 Diagnostics Snapshot</b>")
    lines.append("")
    lines.append(f"<b>Waiters</b>: premium=<code>{waiters.get('premium',0)}</code> | free=<code>{waiters.get('free',0)}</code>")
    lines.append(
        f"<b>Active tasks</b>: <code>{snap['totals']['active']}</code> "
        f"(downloads=<code>{snap['totals']['downloads']}</code>, uploads=<code>{snap['totals']['uploads']}</code>)"
    )
    lines.append(f"<b>Active by tier</b>: premium=<code>{prem_count}</code> | free=<code>{free_count}</code>")
    lines.append("")

    # List running tasks (cap to 15)
    if tasks:
        lines.append("<b>📋 Running tasks (up to 15)</b>:")
        for t in tasks[:15]:
            kind = t.get("kind")
            uid = t.get("user_id")
            uname = t.get("username") or "-"
            link = t.get("link") or "-"
            started = _fmt_ts(t.get("started_at", 0))
            lines.append(f"• [<code>{kind}</code>] uid=<code>{uid}</code> @{uname} | {link} | since <code>{started}</code>")
        lines.append("")

    # Per-session usage
    if diag:
        lines.append("<b>📦 Sessions</b>:")
        for sid, info in diag.items():
            if sid == "_waiters":
                continue
            in_use = info.get("in_use")
            conc = info.get("concurrency")
            usage = info.get("usage_count")
            errors = info.get("errors")
            last = _fmt_ts(info.get("last_used", 0))
            cool = " (cooldown)" if info.get("in_cooldown") else ""
            started = info.get("client_started")
            start_err = info.get("last_start_error") or ""
            lines.append(
                f"• <code>{sid}</code>: in_use=<code>{in_use}/{conc}</code> "
                f"used=<code>{usage}</code> errors=<code>{errors}</code> last=<code>{last}</code>{cool}"
            )
            if started is not None:
                ok = "✅" if started else "❌"
                if started:
                    lines.append(f"   └ client_started: {ok}")
                else:
                    lines.append(f"   └ client_started: {ok} <code>{start_err[:120]}</code>")

    # Tasks grouped per session (active only)
    if per_session:
        lines.append("")
        lines.append("<b>🧩 Tasks per session</b>:")
        for sid, ts in per_session.items():
            lines.append(f"• Session <code>{sid}</code>: <code>{len(ts)}</code> task(s)")
            # Show up to 8 tasks per session
            for t in ts[:8]:
                kind = t.get("kind")
                uid = t.get("user_id")
                uname = t.get("username") or "-"
                link = t.get("link") or "-"
                start = _fmt_ts(t.get("started_at", 0))
                lines.append(f"   - [<code>{kind}</code>] uid=<code>{uid}</code> @{uname} | {link} | since <code>{start}</code>")
        lines.append("")

    text = "\n".join(lines)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")]])
    await message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=kb)
