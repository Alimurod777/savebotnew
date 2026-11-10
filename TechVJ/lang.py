"""
TechVJ/lang.py - Multi-language support for the bot.

Supports: English (en), Uzbek (uz), Russian (ru)
Default: English
"""

import os
import logging
from typing import Dict, Optional
from wzgram import Client, filters
from wzgram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

logger = logging.getLogger(__name__)

# Language storage directory
LANG_DIR = "data/user_lang"
os.makedirs(LANG_DIR, exist_ok=True)

# Default language
DEFAULT_LANG = "en"

# ==================== LANGUAGE STRINGS ====================

STRINGS: Dict[str, Dict[str, str]] = {
    "en": {
        # General
        "lang_select": "🌐 **Select Language**\n\nCurrent: English 🇬🇧",
        "lang_changed": "✅ Language changed to **English** 🇬🇧",
        
        # Start
        "start_msg": "👋 **Welcome!**\n\nSend me a media link or forward a message to save it.",
        "start_help": "Use /help for commands list.",
        
        # Download/Upload
        "downloading": "⬇️ Downloading...",
        "uploading": "⬆️ Uploading...",
        "download_complete": "✅ Download complete!",
        "download_failed": "❌ Download failed",
        "processing": "⏳ Processing...",
        
        # Large file splitting
        "file_too_large": "📦 File is larger than 2GB ({size})\nSplitting...",
        "split_complete": "✅ Split into **{parts}** parts!\nUse file joining software to combine.",
        "splitting_ffmpeg": "✂️ Splitting with FFmpeg (quality preserved)...",
        "splitting_binary": "✂️ Splitting file...",
        "uploading_part": "⬆️ Uploading: **{part}/{total}**\n`{name}` ({size})",
        
        # Caption
        "caption_continued": "📝 Caption continued:",
        "caption_overflow": "📎 Continuation below ↓",
        
        # Progress
        "progress_download": "⬇️ Downloading\n{progress}% | {speed}/s\nETA: {eta}",
        "progress_upload": "⬆️ Uploading\n{progress}% | {speed}/s\nETA: {eta}",
        
        # Errors
        "error_generic": "❌ Error: {error}",
        "error_not_found": "❌ File not found",
        "error_timeout": "⏱️ Request timed out",
        "error_flood": "⚠️ Too many requests. Wait {seconds}s",
        "split_flood_wait": "⏳ Telegram temporarily limited uploads (FloodWait). Please wait {seconds} seconds; I will then automatically continue sending the remaining parts. You do not need to send the link again.",
        "error_split": "❌ Failed to split large file: {error}",
        
        # Login/Logout
        "not_logged_in": "❌ You are not logged in.",
        "already_logged_in": "✅ You are already logged in.\n\nUse /logout first to login with another account.",
        "logout_success": "✅ **Logout Successfully** ♦",
        "login_cancelled": "❌ Login cancelled due to timeout.",
        "process_cancelled": "❌ Process cancelled!",
        "send_phone": "📱 **Send your phone number** in international format.\n\nExample: `+998901234567`",
        "invalid_phone": "❌ Phone number is invalid.",
        "sending_otp": "📤 Sending OTP...",
        "send_otp": "🔑 **Enter the OTP** sent to your phone.\n\nFormat: `1 2 3 4 5` (with spaces)",
        "invalid_otp": "❌ OTP is invalid. Please try again.",
        "otp_expired": "❌ OTP is expired. Please use /login to start again.",
        "otp_max_attempts": "❌ Maximum attempts reached.",
        "send_password": "🔐 **Two-Step Verification**\n\nEnter your password:",
        "invalid_password": "❌ Invalid password. Please try again.",
        "password_max_attempts": "❌ Maximum password attempts reached.",
        "login_success": "✅ **Successfully Logged In!**\n\nNow forward or send links to save media.",
        "login_error": "❌ Login error: {error}",
        "db_unavailable": "⚠️ Database unavailable. Session saved to file.",
        
        # QR Login
        "qr_scan": "📱 **Scan this QR code** with your Telegram app.\n\nGo to Settings → Devices → Scan QR",
        "qr_expired": "❌ QR code expired. Use /login to try again.",
        "qr_waiting": "⏳ Waiting for scan...",
        
        # Album
        "album_processing": "📸 Processing album ({count} items)...",
        "album_complete": "✅ Album sent!",
        "album_error": "❌ Album error: {error}",
        
        # Help
        "help_msg": """📚 **Commands**

/start - Start the bot
/help - Show this message
/lang - Change language 🌐
/login - Login with your account
/logout - Logout

**Usage:**
Send a media link or forward a message to download/save it.""",
        
        # Queue
        "queue_added": "📋 **Added to queue**\n\nPosition: {position}\nCurrently downloading: `{current}`\n\nCancel: /stop",
        "queue_full": "❌ Queue is full (max {max}).\nWait for current downloads to finish.",
        "queue_next": "▶️ **Starting next from queue...**\n`{link}`",
        "queue_from": "📥 From queue: {count} post(s) downloading...",
        "queue_topic": "📥 From queue: Topic {topic_id} downloading...",
        "queue_thread": "📥 From queue: Thread {thread_id} downloading...",
        "queue_public": "📥 From queue: Public channel ({count} posts)",
        "queue_bot": "📥 From queue: Bot chat downloading...",
        "queue_cleared": "📋 Queue cleared: {count}",
        "queue_done": "✅ Done: {done}/{total}",
        "queue_done_errors": "✅ Done: {done}/{total} (errors: {errors})",
        "queue_empty": "📭 Queue is empty",
        "queue_status": "📋 **Queue Status**\n\nActive: {active}\nWaiting: {waiting}",
        "login_server_busy": "⏳ Server busy. Other users are logging in. Please wait 1–2 minutes.",
        "login_use_qr": "*OTP could not be sent to this account.*\n\nThis may be due to Telegram's security requirements.\n\n*Alternative: Use QR Code Login*\nSend /qrlogin to login by scanning a QR code with your phone.\n\n_This method is more reliable and doesn't require OTP._",
        "otp_only": "Send only the verification code. Other commands are unavailable during login. Cancel: /cancel.",
        "otp_digits": "The verification code must contain only digits.",
        "phone_unregistered": "This number is not registered with Telegram. Register in the official app first.",
        "otp_refreshing": "⚠️ Code expired. Sending a new code...",
        "otp_resent": "✅ New code sent. Enter it in this format: 1 2 3 4 5.",
        "otp_resend_failed": "❌ Failed to resend code: {resend_err}\n\nTry /login again.",
        "session_export_failed": "❌ Could not create a session. Use /logout, then /login again.",
        "session_validation_failed": "❌ Session validation failed. Try /login again.",
        "session_invalid": "❌ Session invalid. Log in again with /login.",
        "session_save_failed": "❌ Error saving session: {e}",
        "login_success_detail": "✅ Account connected! You can now save media.\n\nFor AUTH KEY errors, use /logout and /login again.",
        "qr_unavailable": "QR login is unavailable. Use /login to log in by phone number.",
        "qr_generating": "⏳ Generating QR code. Please wait...",
        "qr_login_failed": "❌ Login failed\n\n{value_0}\n\nTry /qrlogin or /login again.",
        "qr_login_error": "❌ Login error: {value_0}\n\nTry /qrlogin or /login again.",
        "no_active_tasks": "❌ No active tasks to cancel.",
        "url_invalid": "Could not parse URL. Please check the format.",
        "url_no_posts": "No post IDs found in URL.",
        "maintenance_notice": "🔧 The bot is under maintenance. Please try again later.",
        "operation_cancelled": "❌ Operation cancelled.",
        "comments_analyzing": "Analyzing comments...",
        "post_missing": "Post not found or deleted.",
        "comments_disabled": "Comments are disabled for this post or thread not found.",
        "chat_inaccessible": "Cannot access this chat. Make sure you are a member.",
        "rate_limited": "Too many requests. Please try again later.",
        "discussion_join_pending": "Join request sent. Try again after a discussion group admin approves it.",
        "discussion_missing": "Discussion group not found or could not be joined. Join the group, then resend the comment link.",
        "connection_error": "❌ Connection error: {e}",
        "connection_error_value": "❌ Connection error: {error}",
        "request_error": "❌ Error: {e}",
        "request_error_short": "❌ Error: {value_0}",
        "rate_retry": "⏳ Try again in {_gov_retry} seconds.",
        "tasks_cancelled": "✅ Cancelled {total} tasks. Use /stop next time.",
        "stop_summary": "✅ All operations stopped\n\n📋 Removed from queue: {queue_cleared}\n⚙️ Tasks: {cancelled_count}\n📥 Download tasks: {engine_cancelled}\n\nDownloads and queue cleared.",
        "comment_usage": "**Usage:** `/comment <post_link>` or `/comments <post_link>`\n\n**Example:**\n`/comment https://t.me/c/123456789/108`\n\nAnalyzes the comment section of a post.",
        "comment_url_invalid": "Invalid URL: {value_0}",
        "post_access_error": "Cannot access post: {e}",
        "comments_error": "Error fetching comments: {e}",
        "comments_none": "**Comment Analysis**\n\n**Channel:** {channel_title}\n**Post ID:** `{post_id}`\n**Comments:** None found\n\nThis post has no comments yet.",
        "comments_found": "Found {total} comments. Processing...",
        "comments_fetching": "Fetching {value_0} comments ({range_start}-{range_end})...",
        "comment_missing": "Comment {single_comment_id} not found or deleted.",
        "comment_nonexistent": "Comment {single_comment_id} does not exist.",
        "comment_progress": "Processing comment {value_0}/{total}...\nDone: {processed} | Failed: {failed}",
        "comments_complete": "**Thread Comments Completed**\nThread ID: {thread_id}\nProcessed: {processed}/{total}\nFailed: {failed}",
        "topic_complete": "**Topic Download Completed**\nTopic ID: {topic_id}\nProcessed: {processed}\nSkipped (not in topic/missing): {skipped}\nFailed: {failed}",
        "topic_missing": "⚠️ Topic not found.\nThe topic ID {topic_id} does not exist or was deleted.",
        "topic_wrong_root": "⚠️ Invalid topic URL.\nMessage {topic_id} is not a topic starter.\nIt appears to be a reply in topic {value_1}.",
        "topic_fetched": "Topic fetched: {total_posts} messages found in topic order. Processing...",
        "topic_anchors_error": "Could not resolve topic range anchors: {_te}",
        "source_timeout": "Source group check timed out: {channel_id}. Try again later.",
        "public_processing": "Processing {total_posts} public posts...",
        "posts_processing": "⏳ Downloading {total_posts} posts...",
        "download_size": "⬇️ Downloading\n{size_mb:.1f} MB",
        "cancelled_count": "**Cancelled**\nProcessed: {processed}/{total}",
        "cancelled_topic": "**Cancelled**\nProcessed: {processed}/{total_posts}\nSkipped: {skipped}",
        "cancelled_albums": "**Cancelled**\nProcessed: {processed}/{total_posts}\nAlbums: {albums_processed}",
        "cancelled_processed": "Cancelled. Processed: {processed}",
        "connection_retry": "**Connection Error**\nPlease try again.\nError: {e}",
        "no_content": "No content to send. Requested: {total_posts}{service_info}",
        "no_messages_retrieved": "⚠️ **No messages retrieved**\nRequested: {total_posts}\n\nMessages may have been deleted or are inaccessible.{service_info}",
        "public_complete_warnings": "✅ **Completed with warnings**\nProcessed: {processed}/{total_posts}\nSome messages could not be retrieved.{service_info}",
        "public_complete": "✅ **Completed**\nProcessed: {processed}/{total_posts}",
        "batch_flood": "⚠️ **FloodWait {wait_time}s** — batch paused.\n✅ {processed}/{total_posts} processed.",
        "topic_flood": "⚠️ **FloodWait {wait_time}s** — batch paused.\n✅ {processed} processed, ❌ {failed} failed\nRetry in {value_3} min.",
        "need_login": "Use /login first to connect your Telegram account and save restricted content.",
        "banned_notice": "🚫 You are banned from using this bot.",
        "unknown_error": "Unknown error",
        "prompt_timeout": "\n\nYou have 5 minutes to reply. Cancel: /cancel.",
        "attempts_remaining": "\n\nAttempts remaining: {count}\nCancel: /cancel.",
        "login_limited": "⏳ Login temporarily limited. Wait {seconds} seconds, then try again. You can also try /qrlogin.",
        "logged_in": "✅ Logged in",
        "logged_out": "❌ Not logged in — use /login",
        "role_line": "\n🎭 Role: {emoji} {role}",
        "role_new_user": "New user",
        "role_normal_user": "User",
        "role_vip_user": "VIP",
        "role_banned_user": "Banned",
        "start_detail": "👋 Hello, {name}!\n\n🤖 Save restricted content\n\n💼 Status: {status}{role}\n\n📎 Link formats:\nhttps://t.me/c/CHANNEL_ID/POST\nhttps://t.me/c/CHANNEL_ID/101-200 (range)\nhttps://t.me/c/CHANNEL_ID/101,102,103 (list)\nhttps://t.me/username/POST (public channel)\n\n📋 Commands:\n/login — Connect your Telegram account\n/logout — Log out\n/status — Profile and limits\n/stop — Stop all operations\n/help — Full help\n/lang — Choose language\n\nConnect your account via /login to use the bot.",
        "session_active": "✅ Active",
        "session_absent": "❌ None",
        "none": "None",
        "account_regular": "Standard",
        "profile_detail": "\n\n👤 Connected profile:\nName: {name}\nUsername: {username}\nID: {id}\nType: {kind}",
        "profile_error": "\n\n⚠️ Could not retrieve profile: {error}",
        "single_only": "❌ Single post only",
        "role_detail": "\n\n🎭 Role: {emoji} {role}\n📊 Limit: {count} requests / {seconds}s\n🔗 Multiple IDs/ranges: {multi}",
        "status_detail": "📊 Your status\n\n🔐 Session: {session}{profile}{role}{queue}\n\nℹ️ Help: /help",
        "status_queue": "\n\n📋 Queue: active {active}, waiting {waiting}",
        "new_user_limit": "❌ New users may send one post at a time. Contact the administrator for access to ranges and multiple IDs.",
        "help_detail": "📚 Help\n\nSend a Telegram link to save its content.\n\nSingle post:\n`https://t.me/c/123456789/108`\n\nMultiple posts:\n`https://t.me/c/123456789/108,114,122`\n\nPost range:\n`https://t.me/c/123456789/100-110`\n\nForum topic (only messages in this topic):\n`https://t.me/c/123456789/5/108`\n`https://t.me/c/123456789/5/100-110`\n\nDiscussion comments (all/range):\n`https://t.me/c/123456789/108?thread=5`\n`https://t.me/c/123456789/108?thread=5&range10-50`\n\n/start — Start\n/help — Help\n/lang — Language (English/O‘zbek/Русский)\n/login — Log in by phone\n/qrlogin — Log in with QR code\n/logout — Log out\n/status — Profile and limits\n/stop — Stop all operations\n/cancel — Cancel operations\n/queue — Download queue\n/info <link> — Channel/group info\n/comment <link> — Analyze comments\n/group <link> — Group details\n/post <link> — Post details\n/uploadsetting — Upload preferences\n/split_media — Split preferences\n\nPhotos, videos, documents, audio, voice and video notes are supported. Photo albums are grouped; mixed media is sent individually. Polls/quizzes include options and correct answers; QuizBot start links are preserved. Large files are split when needed. Private channels require /login first.",
        "qr_token_failed": "Failed to generate QR code token",
        "qr_image_failed": "Failed to create QR code image",
        "qr_timeout": "Login timeout. Please try again.",
        "qr_library_missing": "QR code library not installed. Please install: pip install qrcode[pil] Pillow",
        "qr_login_timeout": "Login cancelled due to timeout.",
        "qr_cancelled": "Login cancelled",
        "qr_password_limit": "Invalid 2FA password. Maximum attempts reached.",
        "qr_export_failed": "Failed to export session",
        "qr_caption": "Scan this QR code in Telegram:\nSettings → Devices → Link Desktop Device\n\nTime remaining: {seconds}s\nRefresh: every {refresh}s\nCancel: /cancel",
        "qr_error_detail": "Login error: {value_0}",
        "qr_2fa_error": "2FA error: {value_0}",
        "queue_heading": "📋 Queue status",
        "queue_current": "▶️ Current: {link}",
        "queue_waiting_count": "\n📝 Waiting: {count}\n",
        "more_count": "... and {count} more",
        "queue_total": "\n📊 Total processed: {count}",
        "upload_setting_detail": "Upload setting: {current}\n\nauto — No split only with your own Premium session\nforce_split — Always split\nno_split — Requires your own Premium session\n\nUsage: /uploadsetting <option>",
        "upload_invalid": "Invalid option: {new_setting}\nValid options: auto, force_split, no_split",
        "upload_setting_saved": "Upload setting changed to {new_setting}",
        "setting_save_failed": "Failed to save setting. Please try again.",
        "own_premium_required": "❌ Disabling splitting requires your own Telegram Premium session. Use /login to connect it. Otherwise files over 2 GB will be split.",
        "split_setting_detail": "Split media: {status}\n\n/split_media on — Always split\n/split_media off — Never split (requires your Premium session)\n/split_media auto — Automatic",
        "split_setting_saved": "Split media: {value_0}",
        "split_invalid": "Invalid option: {choice}\nValid options: on, off, auto",
        "split_mode_on": "ON ✅ — Always split captions",
        "split_mode_off": "OFF ❌ — No splitting (your Premium session)",
        "split_mode_auto": "AUTO 🔄 — Automatic",
        "upload_complete": "✅ Upload complete!",
        "upload_failed": "❌ Upload failed",
        "eta_label": "Remaining",
        "size_label": "Size",
        "time_label": "Time",
        "average_speed_label": "Average speed",
        "calculating": "Calculating...",
        "source_heading": "📡 Source: {title}",
        "source_topic": "\n📂 Topic: {title}",
        "continuation_prefix": "📌 Continued",
        "language_invalid": "Invalid language",
        "language_save_failed": "Could not save language. Please try again.",
        "topic_anchors": "Anchors: {value_0} → {value_1}",
        "failed_ids": "\n\nUndelivered IDs:\n{id_list}",
        "deleted_count": "\n🗑 Deleted: {deleted}",
        "service_skipped": "\nℹ️ Service messages without content: {service_skipped}",
        "message_id": "Message ID: {value_0}",
        "comments_range_thread": "Fetching comments {range_start}–{range_end} from thread {thread_id}...",
        "comments_range": "Fetching comments {range_start}–{range_end}...",
        "comment_thread": "Fetching comment {single_comment_id} from thread {thread_id}...",
        "comment_single": "Fetching comment {single_comment_id}...",
        "topic_processing": "Processing topic {topic_id}...\n{status_detail}",
        "failed_more": "\n... and {value_0} more",
        "topic_progress": "Processing {value_0}/{total_posts}\nDone: {processed} | Skipped: {skipped} | Failed: {failed}",
        "private_stopped": "{stop_summary}\nDelivered posts/albums: {processed}\nRequested IDs: {total_posts}\n📁 Albums: {albums_processed}\n❌ Failed: {failed}{deleted_info}{service_info}{failed_info}",
        "private_deleted": "⚠️ All messages deleted\nRequested: {total_posts}\n🗑 Deleted: {deleted}",
        "private_session_invalid": "Session invalid. Log in again with /login.\nReason: {e}",
        "download_session_invalid": "Download session invalid\n{e}",
        "private_none": "⚠️ No messages delivered\nRequested: {total_posts}\n❌ Failed: {failed}{deleted_info}{service_info}{failed_info}",
        "private_complete_errors": "✅ Complete\nDelivered: {processed}/{total_posts}\n📁 Albums: {albums_processed}\n❌ Failed: {failed}{deleted_info}{service_info}{failed_info}",
        "private_complete": "✅ Complete\nDelivered: {processed}/{total_posts}\n📁 Albums: {albums_processed}{deleted_info}{service_info}",
        "premium_busy": "⚠️ All Premium sessions are busy ({_wait_min} min).\nHow would you like to continue?",
        "batch_pause": "⏸ Pausing {pause:.0f}s... ({idx}/{total_posts})",
        "private_pause": "⏸ Pausing {pause:.0f}s... ({value_1}/{total_posts})",
        "premium_wait": "⏳ Wait ({_wait_min} min)",
        "premium_skip": "⚡ Continue without Premium",
        "batch_errors": "⚠️ Too many errors — batch stopped",
        "private_flood_resume": "⏳ Telegram temporarily limited requests (FloodWait).\nCurrent post ID: {post_id}\nDelivered posts/albums: {processed}\nWait: {wait_time} seconds.\nI will resume automatically from post {post_id}. No need to resend the link.\nCancel: /stop",
        "comment_analysis_detail": "**Comment Analysis**\n\n**Channel:** {channel_title}\n**Post ID:** `{post_id}`\n**Thread ID:** `{thread_id}`\n**Discussion Group:** `{group_id}`\n**First Comment ID:** `{first_comment_id}`\n**Last Comment ID:** `{last_comment_id}`\n**Total Comments:** {total_comments}\n\n**First Comment Link:**\n`https://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}`\n\n**Range Link:**\n`https://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}&range={first_comment_id}-{last_comment_id}`",
        "source_inaccessible": "⚠️ Cannot access the source group.\nGroup ID: {channel_id}\nDownload account: {download_account_id}\nCompare the group ID in the link with the group containing these topics in /userdata. The download account must be a member.\nTelegram error: {value_2}",
        "failure_wait": "Telegram requires a temporary wait.",
        "failure_missing": "The post is deleted or does not exist.",
        "failure_restricted": "The post is restricted or forwarding is not allowed.",
        "failure_timeout": "Timed out while retrieving the post.",
        "failure_system": "An internal bot error occurred while sending the post.",
        "failure_generic": "Could not retrieve or send the post.",
        "failure_skip": "Post skipped.\nPost ID: {post_id}\nReason: {reason}\nMoving to the next post.",
        "session_expired_detail": "Your session expired\nReason: {error}\n\n{relogin}",
        "relogin": "Log in again with /login.",
        "relogin_wait": "Try /login again in one minute if the system is available.",
    },
    
    "uz": {
        # General
        "lang_select": "🌐 **Tilni tanlang**\n\nHozirgi: O'zbek 🇺🇿",
        "lang_changed": "✅ Til **O'zbek**ga o'zgartirildi 🇺🇿",
        
        # Start
        "start_msg": "👋 **Xush kelibsiz!**\n\nMenga media link yuboring yoki xabarni forward qiling.",
        "start_help": "Buyruqlar ro'yxati uchun /help bosing.",
        
        # Download/Upload
        "downloading": "⬇️ Yuklanmoqda...",
        "uploading": "⬆️ Yuborilmoqda...",
        "download_complete": "✅ Yuklash tugadi!",
        "download_failed": "❌ Yuklash xatosi",
        "processing": "⏳ Qayta ishlanmoqda...",
        
        # Large file splitting
        "file_too_large": "📦 Fayl 2GB dan katta ({size})\nBo'linmoqda...",
        "split_complete": "✅ **{parts}** qismga bo'lindi!\nBirlashtirish uchun maxsus dastur ishlating.",
        "splitting_ffmpeg": "✂️ FFmpeg bilan bo'linmoqda (sifat saqlanadi)...",
        "splitting_binary": "✂️ Fayl bo'linmoqda...",
        "uploading_part": "⬆️ Yuklanmoqda: **{part}/{total}**\n`{name}` ({size})",
        
        # Caption
        "caption_continued": "📝 Caption davomi:",
        "caption_overflow": "📎 Davomi pastda ↓",
        
        # Progress
        "progress_download": "⬇️ Yuklanmoqda\n{progress}% | {speed}/s\nQolgan: {eta}",
        "progress_upload": "⬆️ Yuborilmoqda\n{progress}% | {speed}/s\nQolgan: {eta}",
        
        # Errors
        "error_generic": "❌ Xato: {error}",
        "error_not_found": "❌ Fayl topilmadi",
        "error_timeout": "⏱️ Vaqt tugadi",
        "error_flood": "⚠️ Juda ko'p so'rov. {seconds}s kuting",
        "split_flood_wait": "⏳ Telegram yuborishni vaqtincha chekladi (FloodWait). Iltimos, {seconds} soniya kuting. Shundan keyin qolgan bo‘laklarni avtomatik yuborishda davom etaman. Havolani qayta yuborishingiz shart emas.",
        "error_split": "❌ Katta faylni bo'lib bo'lmadi: {error}",
        
        # Login/Logout
        "not_logged_in": "❌ Siz tizimga kirmagansiz.",
        "already_logged_in": "✅ Siz allaqachon tizimdasiz.\n\nBoshqa hisob bilan kirish uchun avval /logout qiling.",
        "logout_success": "✅ **Muvaffaqiyatli chiqdingiz** ♦",
        "login_cancelled": "❌ Vaqt tugagani sababli bekor qilindi.",
        "process_cancelled": "❌ Jarayon bekor qilindi!",
        "send_phone": "📱 **Telefon raqamingizni yuboring** xalqaro formatda.\n\nMisol: `+998901234567`",
        "invalid_phone": "❌ Telefon raqami noto'g'ri.",
        "sending_otp": "📤 OTP yuborilmoqda...",
        "send_otp": "🔑 **Telefoningizga kelgan kodni kiriting.**\n\nFormat: `1 2 3 4 5` (bo'sh joy bilan)",
        "invalid_otp": "❌ Kod noto'g'ri. Qaytadan urinib ko'ring.",
        "otp_expired": "❌ Kod muddati tugagan. Qaytadan /login bosing.",
        "otp_max_attempts": "❌ Maksimal urinish soni tugadi.",
        "send_password": "🔐 **Ikki bosqichli tekshiruv**\n\nParolingizni kiriting:",
        "invalid_password": "❌ Parol noto'g'ri. Qaytadan urinib ko'ring.",
        "password_max_attempts": "❌ Maksimal parol urinishi tugadi.",
        "login_success": "✅ **Muvaffaqiyatli kirdingiz!**\n\nEndi media saqlash uchun link yuboring yoki forward qiling.",
        "login_error": "❌ Kirishda xato: {error}",
        "db_unavailable": "⚠️ Database mavjud emas. Sessiya faylga saqlandi.",
        
        # QR Login
        "qr_scan": "📱 **Bu QR kodni skanerlang** Telegram ilovangiz bilan.\n\nSozlamalar → Qurilmalar → QR skanerlash",
        "qr_expired": "❌ QR kod muddati tugadi. Qaytadan /login bosing.",
        "qr_waiting": "⏳ Skanerlanishi kutilmoqda...",
        
        # Album
        "album_processing": "📸 Album qayta ishlanmoqda ({count} ta)...",
        "album_complete": "✅ Album yuborildi!",
        "album_error": "❌ Album xatosi: {error}",
        
        # Help
        "help_msg": """📚 **Buyruqlar**

/start - Botni ishga tushirish
/help - Yordam
/lang - Tilni o'zgartirish 🌐
/login - Hisobga kirish
/logout - Hisobdan chiqish

**Foydalanish:**
Media link yuboring yoki xabarni forward qiling.""",
        
        # Queue
        "queue_added": "📋 **Navbatga qo'shildi**\n\nO'rni: {position}\nHozir yuklanmoqda: `{current}`\n\nBekor qilish: /stop",
        "queue_full": "❌ Navbat to'lgan (max {max}).\nJoriy yuklanishlar tugashini kuting.",
        "queue_next": "▶️ **Navbatdan keyingi havola...**\n`{link}`",
        "queue_from": "📥 Navbatdan: {count} ta post yuklanmoqda...",
        "queue_topic": "📥 Navbatdan: Topic {topic_id} yuklanmoqda...",
        "queue_thread": "📥 Navbatdan: Thread {thread_id} yuklanmoqda...",
        "queue_public": "📥 Navbatdan: Public kanal ({count} ta post)",
        "queue_bot": "📥 Navbatdan: Bot chat yuklanmoqda...",
        "queue_cleared": "📋 Navbatdan o'chirildi: {count}",
        "queue_done": "✅ Tugadi: {done}/{total}",
        "queue_done_errors": "✅ Tugadi: {done}/{total} (xato: {errors})",
        "queue_empty": "📭 Navbat bo'sh",
        "queue_status": "📋 **Navbat holati**\n\nFaol: {active}\nKutmoqda: {waiting}",
        "login_server_busy": "⏳ Server band. Boshqa foydalanuvchilar hisobga kirmoqda. 1–2 daqiqa kuting.",
        "login_use_qr": "Bu hisobga tasdiqlash kodini yuborib bo‘lmadi. Telegram xavfsizlik talabi sabab bo‘lishi mumkin.\n\nQR-kod orqali kirish uchun /qrlogin buyrug‘idan foydalaning.",
        "otp_only": "Faqat tasdiqlash kodini yuboring. Kirish paytida boshqa buyruqlar ishlamaydi. Bekor qilish: /cancel.",
        "otp_digits": "Tasdiqlash kodi faqat raqamlardan iborat bo‘lishi kerak.",
        "phone_unregistered": "Bu raqam Telegramda ro‘yxatdan o‘tmagan. Avval rasmiy Telegram ilovasida ro‘yxatdan o‘ting.",
        "otp_refreshing": "⚠️ Kod muddati tugagan. Yangi kod yuborilmoqda...",
        "otp_resent": "✅ Yangi kod yuborildi. Uni quyidagi formatda kiriting: 1 2 3 4 5.",
        "otp_resend_failed": "❌ Yangi kod yuborishda xato: {resend_err}\n\nQaytadan /login buyrug‘ini yuboring.",
        "session_export_failed": "❌ Sessiya yaratib bo‘lmadi. /logout qilib, qaytadan /login buyrug‘i bilan kiring.",
        "session_validation_failed": "❌ Sessiya tekshiruvdan o‘tmadi. /login bilan qaytadan urinib ko‘ring.",
        "session_invalid": "❌ Sessiya yaroqsiz. /login bilan qayta kiring.",
        "session_save_failed": "❌ Sessiyani saqlashda xato: {e}",
        "login_success_detail": "✅ Hisobingiz ulandi! Endi media saqlashingiz mumkin.\n\nAUTH KEY xatosi chiqsa, /logout qilib, qaytadan /login bilan kiring.",
        "qr_unavailable": "QR orqali kirish mavjud emas. Telefon raqami bilan kirish uchun /login yuboring.",
        "qr_generating": "⏳ QR-kod yaratilmoqda. Biroz kuting...",
        "qr_login_failed": "❌ Kirish amalga oshmadi\n\n{value_0}\n\nQaytadan /qrlogin yoki /login buyrug‘ini yuboring.",
        "qr_login_error": "❌ Kirishda xato: {value_0}\n\nQaytadan /qrlogin yoki /login bilan urinib ko‘ring.",
        "no_active_tasks": "❌ Bekor qilinadigan faol vazifa yo‘q.",
        "url_invalid": "Havolani o‘qib bo‘lmadi. Formatini tekshiring.",
        "url_no_posts": "Havolada post ID topilmadi.",
        "maintenance_notice": "🔧 Bot texnik xizmatda. Keyinroq urinib ko‘ring.",
        "operation_cancelled": "❌ Jarayon bekor qilindi.",
        "comments_analyzing": "Izohlar tahlil qilinmoqda...",
        "post_missing": "Post topilmadi yoki o‘chirilgan.",
        "comments_disabled": "Bu postda izohlar o‘chirilgan yoki izohlar tarmog‘i topilmadi.",
        "chat_inaccessible": "Chatga kirib bo‘lmadi. Unga a’zo ekaningizni tekshiring.",
        "rate_limited": "So‘rovlar juda ko‘p. Keyinroq urinib ko‘ring.",
        "discussion_join_pending": "Muhokama guruhiga kirish so‘rovi yuborildi. Admin tasdiqlagach qayta urinib ko‘ring.",
        "discussion_missing": "Muhokama guruhi topilmadi yoki unga kirib bo‘lmadi. Guruhga a’zo bo‘lib, izoh havolasini qayta yuboring.",
        "connection_error": "❌ Ulanishda xato: {e}",
        "connection_error_value": "❌ Ulanishda xato: {error}",
        "request_error": "❌ Xato: {e}",
        "request_error_short": "❌ Xato: {value_0}",
        "rate_retry": "⏳ {_gov_retry} soniyadan keyin qayta urinib ko‘ring.",
        "tasks_cancelled": "✅ {total} ta vazifa bekor qilindi. Keyingi safar /stop buyrug‘idan foydalaning.",
        "stop_summary": "✅ Barcha operatsiyalar to‘xtatildi\n\n📋 Navbatdan o‘chirildi: {queue_cleared}\n⚙️ Vazifalar: {cancelled_count}\n📥 Yuklash vazifalari: {engine_cancelled}\n\nYuklanishlar va navbat tozalandi.",
        "comment_usage": "Foydalanish: /comment <post_link> yoki /comments <post_link>\n\nMisol:\n/comment https://t.me/c/123456789/108\n\nPost izohlarini tahlil qiladi.",
        "comment_url_invalid": "Havola noto‘g‘ri: {value_0}",
        "post_access_error": "Postga kirib bo‘lmadi: {e}",
        "comments_error": "Izohlarni olishda xato: {e}",
        "comments_none": "Izohlar tahlili\n\nKanal: {channel_title}\nPost ID: {post_id}\n\nBu postda hali izohlar yo‘q.",
        "comments_found": "{total} ta izoh topildi. Qayta ishlanmoqda...",
        "comments_fetching": "{value_0} ta izoh olinmoqda ({range_start}–{range_end})...",
        "comment_missing": "{single_comment_id}-izoh topilmadi yoki o‘chirilgan.",
        "comment_nonexistent": "{single_comment_id}-izoh mavjud emas.",
        "comment_progress": "Izoh: {value_0}/{total}\nYuborildi: {processed} | Xato: {failed}",
        "comments_complete": "✅ Izohlar yuklandi\nTarmoq ID: {thread_id}\nQayta ishlandi: {processed}/{total}\nXato: {failed}",
        "topic_complete": "✅ Topik yuklandi\nTopik ID: {topic_id}\nQayta ishlandi: {processed}\nO‘tkazib yuborildi (boshqa topik/topilmadi): {skipped}\nXato: {failed}",
        "topic_missing": "⚠️ {topic_id}-topik mavjud emas yoki o‘chirilgan.",
        "topic_wrong_root": "⚠️ Topik havolasi noto‘g‘ri.\n{topic_id}-xabar topik boshi emas.\nU {value_1}-topikdagi javob bo‘lishi mumkin.",
        "topic_fetched": "Topikda {total_posts} ta xabar topildi. Qayta ishlanmoqda...",
        "topic_anchors_error": "Topik diapazoni chegaralarini aniqlab bo‘lmadi: {_te}",
        "source_timeout": "{channel_id}-guruhni tekshirish vaqti tugadi. Keyinroq urinib ko‘ring.",
        "public_processing": "{total_posts} ta ochiq post qayta ishlanmoqda...",
        "posts_processing": "⏳ {total_posts} ta post yuklanmoqda...",
        "download_size": "⬇️ Yuklanmoqda\n{size_mb:.1f} MB",
        "cancelled_count": "❌ Bekor qilindi\nQayta ishlandi: {processed}/{total}",
        "cancelled_topic": "❌ Bekor qilindi\nQayta ishlandi: {processed}/{total_posts}\nO‘tkazildi: {skipped}",
        "cancelled_albums": "❌ Bekor qilindi\nQayta ishlandi: {processed}/{total_posts}\nAlbumlar: {albums_processed}",
        "cancelled_processed": "❌ Bekor qilindi. Qayta ishlandi: {processed}",
        "connection_retry": "❌ Ulanishda xato. Qayta urinib ko‘ring.\nXato: {e}",
        "no_content": "Yuboriladigan kontent yo‘q. So‘ralgan: {total_posts}{service_info}",
        "no_messages_retrieved": "⚠️ Xabarlar olinmadi\nSo‘ralgan: {total_posts}\n\nXabarlar o‘chirilgan yoki ularga kirish yo‘q.{service_info}",
        "public_complete_warnings": "✅ Ogohlantirishlar bilan tugadi\nQayta ishlandi: {processed}/{total_posts}\nAyrim xabarlarni olib bo‘lmadi.{service_info}",
        "public_complete": "✅ Tugadi\nQayta ishlandi: {processed}/{total_posts}",
        "batch_flood": "⏳ Telegram cheklovi: {wait_time} soniya. Jarayon to‘xtatib turildi.\n✅ Qayta ishlandi: {processed}/{total_posts}.",
        "topic_flood": "⏳ Telegram cheklovi: {wait_time} soniya. Jarayon to‘xtatib turildi.\n✅ Qayta ishlandi: {processed}, ❌ xato: {failed}\n{value_3} daqiqadan keyin qayta urinib ko‘ring.",
        "need_login": "Yopiq kontentni saqlash uchun avval /login orqali Telegram hisobingizni ulang.",
        "banned_notice": "🚫 Sizga botdan foydalanish taqiqlangan.",
        "unknown_error": "Noma’lum xato",
        "prompt_timeout": "\n\nJavob berish uchun 5 daqiqa. Bekor qilish: /cancel.",
        "attempts_remaining": "\n\nQolgan urinishlar: {count}\nBekor qilish: /cancel.",
        "login_limited": "⏳ Kirish vaqtincha cheklangan. {seconds} soniya kutib, qayta urinib ko‘ring. /qrlogin usulini ham sinab ko‘rishingiz mumkin.",
        "logged_in": "✅ Hisobga kirilgan",
        "logged_out": "❌ Hisobga kirilmagan — /login yuboring",
        "role_line": "\n🎭 Rolingiz: {emoji} {role}",
        "role_new_user": "Yangi foydalanuvchi",
        "role_normal_user": "Foydalanuvchi",
        "role_vip_user": "VIP",
        "role_banned_user": "Bloklangan",
        "start_detail": "👋 Salom, {name}!\n\n🤖 Cheklangan kontentni saqlash\n\n💼 Holat: {status}{role}\n\n📎 Havola formatlari:\nhttps://t.me/c/CHANNEL_ID/POST\nhttps://t.me/c/CHANNEL_ID/101-200 (diapazon)\nhttps://t.me/c/CHANNEL_ID/101,102,103 (ro‘yxat)\nhttps://t.me/username/POST (ochiq kanal)\n\n📋 Buyruqlar:\n/login — Telegram hisobingizni ulash\n/logout — Hisobdan chiqish\n/status — Profil va limitlar\n/stop — Barcha operatsiyalarni to‘xtatish\n/help — To‘liq yordam\n/lang — Tilni tanlash\n\nBotdan foydalanish uchun /login orqali hisobingizni ulang.",
        "session_active": "✅ Faol",
        "session_absent": "❌ Yo‘q",
        "none": "Yo‘q",
        "account_regular": "Oddiy",
        "profile_detail": "\n\n👤 Ulangan profil:\nIsm: {name}\nUsername: {username}\nID: {id}\nTuri: {kind}",
        "profile_error": "\n\n⚠️ Profilni olishda xato: {error}",
        "single_only": "❌ Faqat bitta post",
        "role_detail": "\n\n🎭 Rol: {emoji} {role}\n📊 Limit: {count} so‘rov / {seconds} soniya\n🔗 Ko‘p ID/diapazon: {multi}",
        "status_detail": "📊 Sizning holatingiz\n\n🔐 Sessiya: {session}{profile}{role}{queue}\n\nℹ️ Yordam: /help",
        "status_queue": "\n\n📋 Navbat: faol {active}, kutmoqda {waiting}",
        "new_user_limit": "❌ Yangi foydalanuvchilar bir vaqtda bitta post yuborishi mumkin. Diapazon va ko‘p IDlar uchun administratorga murojaat qiling.",
        "help_detail": "📚 Yordam\n\nKontentni saqlash uchun Telegram havolasini yuboring.\n\nBitta post:\n`https://t.me/c/123456789/108`\n\nBir nechta post:\n`https://t.me/c/123456789/108,114,122`\n\nPostlar diapazoni:\n`https://t.me/c/123456789/100-110`\n\nForum topigi (faqat shu topik xabarlari):\n`https://t.me/c/123456789/5/108`\n`https://t.me/c/123456789/5/100-110`\n\nMuhokama izohlari (barchasi/diapazon):\n`https://t.me/c/123456789/108?thread=5`\n`https://t.me/c/123456789/108?thread=5&range10-50`\n\n/start — Boshlash\n/help — Yordam\n/lang — Til (English/O‘zbek/Русский)\n/login — Telefon orqali kirish\n/qrlogin — QR-kod orqali kirish\n/logout — Hisobdan chiqish\n/status — Profil va limitlar\n/stop — Barcha operatsiyalarni to‘xtatish\n/cancel — Operatsiyalarni bekor qilish\n/queue — Yuklash navbati\n/info <link> — Kanal/guruh ma’lumoti\n/comment <link> — Izohlar tahlili\n/group <link> — Guruh tafsilotlari\n/post <link> — Post tafsilotlari\n/uploadsetting — Yuborish sozlamasi\n/split_media — Bo‘lish sozlamasi\n\nRasm, video, hujjat, audio, ovozli va dumaloq video xabarlar qo‘llanadi. Rasm albomlari guruhlanadi, aralash media alohida yuboriladi. So‘rovnoma va testlarda variantlar va to‘g‘ri javoblar ko‘rsatiladi; QuizBot boshlash havolasi saqlanadi. Katta fayllar zarur bo‘lsa bo‘linadi. Yopiq kanallar uchun avval /login orqali kiring.",
        "qr_token_failed": "QR-kod tokenini yaratib bo‘lmadi.",
        "qr_image_failed": "QR-kod rasmini yaratib bo‘lmadi.",
        "qr_timeout": "Kirish vaqti tugadi. Qayta urinib ko‘ring.",
        "qr_library_missing": "QR orqali kirish mavjud emas. /login buyrug‘idan foydalaning.",
        "qr_login_timeout": "Kirish vaqti tugagani uchun bekor qilindi.",
        "qr_cancelled": "Kirish bekor qilindi.",
        "qr_password_limit": "Parol noto‘g‘ri. Urinishlar soni tugadi.",
        "qr_export_failed": "Sessiya yaratib bo‘lmadi. /login bilan qayta kiring.",
        "qr_caption": "QR-kodni Telegramda skanerlang:\nSozlamalar → Qurilmalar → Qurilmani ulash\n\nQolgan vaqt: {seconds} soniya\nYangilanish: har {refresh} soniyada\nBekor qilish: /cancel",
        "qr_error_detail": "Kirishda xato: {value_0}",
        "qr_2fa_error": "Kirishda xato: {value_0}",
        "queue_heading": "📋 Navbat holati",
        "queue_current": "▶️ Hozir: {link}",
        "queue_waiting_count": "\n📝 Navbatda: {count}\n",
        "more_count": "... va yana {count} ta",
        "queue_total": "\n📊 Jami qayta ishlangan: {count}",
        "upload_setting_detail": "Yuborish sozlamasi: {current}\n\nauto — Faqat o‘z Premium sessiyangiz bilan bo‘linmaydi\nforce_split — Doim bo‘linadi\nno_split — O‘z Premium sessiyangiz kerak\n\nFoydalanish: /uploadsetting <option>",
        "upload_invalid": "Noto‘g‘ri tanlov: {new_setting}\nMavjud tanlovlar: auto, force_split, no_split",
        "upload_setting_saved": "Yuborish sozlamasi o‘zgartirildi: {new_setting}",
        "setting_save_failed": "Sozlamani saqlab bo‘lmadi. Qayta urinib ko‘ring.",
        "own_premium_required": "❌ Bo‘lishni o‘chirish uchun o‘z Telegram Premium sessiyangiz kerak. Uni /login orqali ulang. Aks holda 2 GB dan katta fayllar bo‘linadi.",
        "split_setting_detail": "Medianing bo‘linishi: {status}\n\n/split_media on — Doim bo‘lish\n/split_media off — Bo‘lmaslik (o‘z Premium sessiyangiz kerak)\n/split_media auto — Avtomatik",
        "split_setting_saved": "Medianing bo‘linishi: {value_0}",
        "split_invalid": "Noto‘g‘ri tanlov: {choice}\nMavjud tanlovlar: on, off, auto",
        "split_mode_on": "ON ✅ — Caption doim bo‘linadi",
        "split_mode_off": "OFF ❌ — Bo‘linmaydi (o‘z Premium sessiyangiz)",
        "split_mode_auto": "AUTO 🔄 — Avtomatik",
        "upload_complete": "✅ Yuborish tugadi!",
        "upload_failed": "❌ Yuborish amalga oshmadi",
        "eta_label": "Qolgan vaqt",
        "size_label": "Hajm",
        "time_label": "Vaqt",
        "average_speed_label": "O‘rtacha tezlik",
        "calculating": "Hisoblanmoqda...",
        "source_heading": "📡 Manba: {title}",
        "source_topic": "\n📂 Topik: {title}",
        "continuation_prefix": "📌 Davomi",
        "language_invalid": "Noto‘g‘ri til",
        "language_save_failed": "Tilni saqlab bo‘lmadi. Qayta urinib ko‘ring.",
        "topic_anchors": "Chegaralar: {value_0} → {value_1}",
        "failed_ids": "\n\nJo'natilmagan ID lar:\n{id_list}",
        "deleted_count": "\n🗑 O'chirilgan: {deleted}",
        "service_skipped": "\nℹ️ Kontentsiz xizmat xabarlari: {service_skipped}",
        "message_id": "Xabar ID: {value_0}",
        "comments_range_thread": "{thread_id}-tarmoqdan {range_start}–{range_end} izohlar olinmoqda...",
        "comments_range": "{range_start}–{range_end} izohlar olinmoqda...",
        "comment_thread": "{thread_id}-tarmoqdan {single_comment_id}-izoh olinmoqda...",
        "comment_single": "{single_comment_id}-izoh olinmoqda...",
        "topic_processing": "Topik qayta ishlanmoqda: {topic_id}...\n{status_detail}",
        "failed_more": "\n...va yana {value_0} ta",
        "topic_progress": "Qayta ishlanmoqda: {value_0}/{total_posts}\nYuborildi: {processed} | O‘tkazildi: {skipped} | Xato: {failed}",
        "private_stopped": "{stop_summary}\nYuborilgan post/album: {processed}\nSo‘ralgan IDlar: {total_posts}\n📁 Albumlar: {albums_processed}\n❌ Xato: {failed}{deleted_info}{service_info}{failed_info}",
        "private_deleted": "⚠️ Barcha xabarlar o‘chirilgan\nSo‘ralgan: {total_posts}\n🗑 O'chirilgan: {deleted}",
        "private_session_invalid": "Sessiya yaroqsiz. /login bilan qayta kiring.\nSabab: {e}",
        "download_session_invalid": "Yuklab olish sessiyasi yaroqsiz\n{e}",
        "private_none": "⚠️ Hech qanday xabar jo'natilmadi\nSo‘ralgan: {total_posts}\n❌ Xato: {failed}{deleted_info}{service_info}{failed_info}",
        "private_complete_errors": "✅ Tugadi\nYuklandi: {processed}/{total_posts}\n📁 Albumlar: {albums_processed}\n❌ Xato: {failed}{deleted_info}{service_info}{failed_info}",
        "private_complete": "✅ Tugadi\nYuklandi: {processed}/{total_posts}\n📁 Albumlar: {albums_processed}{deleted_info}{service_info}",
        "premium_busy": "⚠️ Barcha Premium sessiyalar band ({_wait_min} daqiqa).\nQanday davom etamiz?",
        "batch_pause": "⏸ Tanaffus: {pause:.0f} soniya... ({idx}/{total_posts})",
        "private_pause": "⏸ Tanaffus: {pause:.0f} soniya... ({value_1}/{total_posts})",
        "premium_wait": "⏳ Kutaman ({_wait_min} daq)",
        "premium_skip": "⚡ Premiumsiz davom",
        "batch_errors": "⚠️ Ko'p xato — jarayon to‘xtatildi",
        "private_flood_resume": "⏳ Telegram vaqtinchalik cheklov qo'ydi (FloodWait).\nYetib kelgan post ID: {post_id}\nYuborilgan post/album: {processed}\nKutish: {wait_time} soniya.\nKutish tugagach, aynan {post_id}-postdan avtomatik davom etaman. Havolani qayta yuborishingiz shart emas.\nBekor qilish: /stop",
        "comment_analysis_detail": "Izohlar tahlili\n\nKanal: {channel_title}\nPost ID: {post_id}\nTarmoq ID: {thread_id}\nMuhokama guruhi: {group_id}\nBirinchi izoh ID: {first_comment_id}\nOxirgi izoh ID: {last_comment_id}\nIzohlar soni: {total_comments}\n\nBirinchi izoh havolasi:\nhttps://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}\n\nDiapazon havolasi:\nhttps://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}&range={first_comment_id}-{last_comment_id}",
        "source_inaccessible": "⚠️ Manba guruhiga kirish imkoni yo'q.\nGuruh ID: {channel_id}\nYuklab olish akkaunti: {download_account_id}\nHavoladagi guruh ID sini /userdata hisobotidagi aynan shu topiklar joylashgan guruh bilan solishtiring. Yuklab olish akkaunti o'sha guruhga a'zo bo'lishi kerak.\nTelegram xatosi: {value_2}",
        "failure_wait": "Telegram vaqtinchalik kutishni talab qildi.",
        "failure_missing": "Post o'chirilgan yoki mavjud emas.",
        "failure_restricted": "Post cheklangan yoki yuborishga ruxsat yo'q.",
        "failure_timeout": "Postni olish vaqti tugadi.",
        "failure_system": "Postni yuborishda botning ichki xatosi yuz berdi.",
        "failure_generic": "Postni olish yoki yuborishda xatolik yuz berdi.",
        "failure_skip": "Post o'tkazib yuborildi.\nPost ID: {post_id}\nSabab: {reason}\nKeyingi postga o'tyapman.",
        "session_expired_detail": "Sessiyangiz muddati tugagan\nSabab: {error}\n\n{relogin}",
        "relogin": "/login bilan qayta kiring.",
        "relogin_wait": "Tizim band bo'lmasa 1 daqiqadan keyin /login bilan qayta kiring.",
    },
    "ru": {
        # General
        "lang_select": "🌐 **Выберите язык**\n\nТекущий: Русский 🇷🇺",
        "lang_changed": "✅ Язык изменён на **Русский** 🇷🇺",

        # Start
        "start_msg": "👋 **Добро пожаловать!**\n\nОтправьте ссылку на медиа или перешлите сообщение, чтобы сохранить его.",
        "start_help": "Список команд: /help.",

        # Download/Upload
        "downloading": "⬇️ Скачивание...",
        "uploading": "⬆️ Отправка...",
        "download_complete": "✅ Скачивание завершено!",
        "download_failed": "❌ Не удалось скачать файл",
        "processing": "⏳ Обработка...",

        # Large file splitting
        "file_too_large": "📦 Файл больше 2 ГБ ({size})\nРазделение...",
        "split_complete": "✅ Файл разделён на **{parts}** частей!\nДля объединения используйте программу для сборки файлов.",
        "splitting_ffmpeg": "✂️ Разделение с помощью FFmpeg (качество сохраняется)...",
        "splitting_binary": "✂️ Разделение файла...",
        "uploading_part": "⬆️ Отправка: **{part}/{total}**\n`{name}` ({size})",

        # Caption
        "caption_continued": "📝 Продолжение подписи:",
        "caption_overflow": "📎 Продолжение ниже ↓",

        # Progress
        "progress_download": "⬇️ Скачивание\n{progress}% | {speed}/с\nОсталось: {eta}",
        "progress_upload": "⬆️ Отправка\n{progress}% | {speed}/с\nОсталось: {eta}",

        # Errors
        "error_generic": "❌ Ошибка: {error}",
        "error_not_found": "❌ Файл не найден",
        "error_timeout": "⏱️ Время ожидания истекло",
        "error_flood": "⚠️ Слишком много запросов. Подождите {seconds} с",
        "split_flood_wait": "⏳ Telegram временно ограничил отправку (FloodWait). Подождите {seconds} секунд. После этого отправка оставшихся частей продолжится автоматически. Повторно отправлять ссылку не нужно.",
        "error_split": "❌ Не удалось разделить большой файл: {error}",

        # Login/Logout
        "not_logged_in": "❌ Вы не вошли в аккаунт.",
        "already_logged_in": "✅ Вы уже вошли в аккаунт.\n\nЧтобы войти в другой аккаунт, сначала выполните /logout.",
        "logout_success": "✅ **Вы вышли из аккаунта** ♦",
        "login_cancelled": "❌ Вход отменён: время ожидания истекло.",
        "process_cancelled": "❌ Процесс отменён!",
        "send_phone": "📱 **Отправьте номер телефона** в международном формате.\n\nПример: `+998901234567`",
        "invalid_phone": "❌ Неверный номер телефона.",
        "sending_otp": "📤 Отправка кода подтверждения...",
        "send_otp": "🔑 **Введите код подтверждения**, полученный на телефон.\n\nФормат: `1 2 3 4 5` (через пробел)",
        "invalid_otp": "❌ Неверный код. Попробуйте ещё раз.",
        "otp_expired": "❌ Срок действия кода истёк. Начните заново: /login.",
        "otp_max_attempts": "❌ Достигнуто максимальное число попыток.",
        "send_password": "🔐 **Двухэтапная аутентификация**\n\nВведите пароль:",
        "invalid_password": "❌ Неверный пароль. Попробуйте ещё раз.",
        "password_max_attempts": "❌ Достигнуто максимальное число попыток ввода пароля.",
        "login_success": "✅ **Вход выполнен!**\n\nТеперь отправьте ссылку или перешлите сообщение, чтобы сохранить медиа.",
        "login_error": "❌ Ошибка входа: {error}",
        "db_unavailable": "⚠️ База данных недоступна. Сессия сохранена в файл.",

        # QR Login
        "qr_scan": "📱 **Отсканируйте этот QR-код** в приложении Telegram.\n\nНастройки → Устройства → Сканировать QR-код",
        "qr_expired": "❌ Срок действия QR-кода истёк. Попробуйте снова: /login.",
        "qr_waiting": "⏳ Ожидание сканирования...",

        # Album
        "album_processing": "📸 Обработка альбома ({count} элементов)...",
        "album_complete": "✅ Альбом отправлен!",
        "album_error": "❌ Ошибка альбома: {error}",

        # Help
        "help_msg": """📚 **Команды**

/start - Запустить бота
/help - Помощь
/lang - Изменить язык 🌐
/login - Войти в аккаунт
/logout - Выйти из аккаунта

**Использование:**
Отправьте ссылку на медиа или перешлите сообщение, чтобы скачать или сохранить его.""",

        # Queue
        "queue_added": "📋 **Добавлено в очередь**\n\nПозиция: {position}\nСейчас скачивается: `{current}`\n\nОтменить: /stop",
        "queue_full": "❌ Очередь заполнена (максимум {max}).\nДождитесь завершения текущих загрузок.",
        "queue_next": "▶️ **Следующая ссылка из очереди...**\n`{link}`",
        "queue_from": "📥 Из очереди: скачивание постов ({count})...",
        "queue_topic": "📥 Из очереди: скачивание темы {topic_id}...",
        "queue_thread": "📥 Из очереди: скачивание ветки {thread_id}...",
        "queue_public": "📥 Из очереди: публичный канал ({count} постов)",
        "queue_bot": "📥 Из очереди: скачивание чата с ботом...",
        "queue_cleared": "📋 Удалено из очереди: {count}",
        "queue_done": "✅ Готово: {done}/{total}",
        "queue_done_errors": "✅ Готово: {done}/{total} (ошибок: {errors})",
        "queue_empty": "📭 Очередь пуста",
        "queue_status": "📋 **Состояние очереди**\n\nАктивных: {active}\nОжидающих: {waiting}",
        "login_server_busy": "⏳ Сервер занят: другие пользователи входят в аккаунт. Подождите 1–2 минуты.",
        "login_use_qr": "Не удалось отправить код подтверждения для этого аккаунта. Причиной могут быть требования безопасности Telegram.\n\nИспользуйте /qrlogin для входа по QR-коду.",
        "otp_only": "Отправьте только код подтверждения. Другие команды во время входа недоступны. Отмена: /cancel.",
        "otp_digits": "Код подтверждения должен содержать только цифры.",
        "phone_unregistered": "Этот номер не зарегистрирован в Telegram. Сначала зарегистрируйтесь в официальном приложении.",
        "otp_refreshing": "⚠️ Срок действия кода истёк. Отправляется новый код...",
        "otp_resent": "✅ Новый код отправлен. Введите его в формате: 1 2 3 4 5.",
        "otp_resend_failed": "❌ Не удалось отправить новый код: {resend_err}\n\nПопробуйте снова: /login.",
        "session_export_failed": "❌ Не удалось создать сессию. Выполните /logout, затем войдите через /login.",
        "session_validation_failed": "❌ Сессия не прошла проверку. Попробуйте войти снова: /login.",
        "session_invalid": "❌ Сессия недействительна. Войдите снова: /login.",
        "session_save_failed": "❌ Ошибка сохранения сессии: {e}",
        "login_success_detail": "✅ Аккаунт подключён! Теперь вы можете сохранять медиа.\n\nПри ошибке AUTH KEY выполните /logout и войдите снова через /login.",
        "qr_unavailable": "Вход по QR-коду недоступен. Используйте /login для входа по номеру телефона.",
        "qr_generating": "⏳ Создаётся QR-код. Подождите...",
        "qr_login_failed": "❌ Не удалось войти\n\n{value_0}\n\nПопробуйте снова: /qrlogin или /login.",
        "qr_login_error": "❌ Ошибка входа: {value_0}\n\nПопробуйте снова: /qrlogin или /login.",
        "no_active_tasks": "❌ Нет активных задач для отмены.",
        "url_invalid": "Не удалось разобрать ссылку. Проверьте формат.",
        "url_no_posts": "В ссылке не найдены ID постов.",
        "maintenance_notice": "🔧 Бот на техническом обслуживании. Попробуйте позже.",
        "operation_cancelled": "❌ Операция отменена.",
        "comments_analyzing": "Анализ комментариев...",
        "post_missing": "Пост не найден или удалён.",
        "comments_disabled": "Комментарии к этому посту отключены или ветка не найдена.",
        "chat_inaccessible": "Нет доступа к чату. Убедитесь, что вы в нём состоите.",
        "rate_limited": "Слишком много запросов. Попробуйте позже.",
        "discussion_join_pending": "Заявка на вступление в группу обсуждения отправлена. Попробуйте после одобрения администратором.",
        "discussion_missing": "Группа обсуждения не найдена или недоступна. Вступите в группу и отправьте ссылку на комментарий снова.",
        "connection_error": "❌ Ошибка соединения: {e}",
        "connection_error_value": "❌ Ошибка соединения: {error}",
        "request_error": "❌ Ошибка: {e}",
        "request_error_short": "❌ Ошибка: {value_0}",
        "rate_retry": "⏳ Попробуйте снова через {_gov_retry} с.",
        "tasks_cancelled": "✅ Отменено задач: {total}. В следующий раз используйте /stop.",
        "stop_summary": "✅ Все операции остановлены\n\n📋 Удалено из очереди: {queue_cleared}\n⚙️ Задач: {cancelled_count}\n📥 Задач скачивания: {engine_cancelled}\n\nЗагрузки и очередь очищены.",
        "comment_usage": "Использование: /comment <post_link> или /comments <post_link>\n\nПример:\n/comment https://t.me/c/123456789/108\n\nАнализирует комментарии к посту.",
        "comment_url_invalid": "Неверная ссылка: {value_0}",
        "post_access_error": "Нет доступа к посту: {e}",
        "comments_error": "Ошибка получения комментариев: {e}",
        "comments_none": "Анализ комментариев\n\nКанал: {channel_title}\nID поста: {post_id}\n\nУ этого поста пока нет комментариев.",
        "comments_found": "Найдено комментариев: {total}. Обработка...",
        "comments_fetching": "Получение комментариев: {value_0} ({range_start}–{range_end})...",
        "comment_missing": "Комментарий {single_comment_id} не найден или удалён.",
        "comment_nonexistent": "Комментарий {single_comment_id} не существует.",
        "comment_progress": "Комментарий: {value_0}/{total}\nОтправлено: {processed} | Ошибок: {failed}",
        "comments_complete": "✅ Комментарии загружены\nID ветки: {thread_id}\nОбработано: {processed}/{total}\nОшибок: {failed}",
        "topic_complete": "✅ Тема загружена\nID темы: {topic_id}\nОбработано: {processed}\nПропущено (другая тема/не найдено): {skipped}\nОшибок: {failed}",
        "topic_missing": "⚠️ Тема {topic_id} не существует или удалена.",
        "topic_wrong_root": "⚠️ Неверная ссылка на тему.\nСообщение {topic_id} не является началом темы.\nВозможно, это ответ в теме {value_1}.",
        "topic_fetched": "В теме найдено сообщений: {total_posts}. Обработка...",
        "topic_anchors_error": "Не удалось определить границы диапазона темы: {_te}",
        "source_timeout": "Время проверки группы {channel_id} истекло. Попробуйте позже.",
        "public_processing": "Обработка публичных постов: {total_posts}...",
        "posts_processing": "⏳ Скачивание постов: {total_posts}...",
        "download_size": "⬇️ Скачивание\n{size_mb:.1f} MB",
        "cancelled_count": "❌ Отменено\nОбработано: {processed}/{total}",
        "cancelled_topic": "❌ Отменено\nОбработано: {processed}/{total_posts}\nПропущено: {skipped}",
        "cancelled_albums": "❌ Отменено\nОбработано: {processed}/{total_posts}\nАльбомов: {albums_processed}",
        "cancelled_processed": "❌ Отменено. Обработано: {processed}",
        "connection_retry": "❌ Ошибка соединения. Попробуйте снова.\nОшибка: {e}",
        "no_content": "Нет контента для отправки. Запрошено: {total_posts}{service_info}",
        "no_messages_retrieved": "⚠️ Сообщения не получены\nЗапрошено: {total_posts}\n\nСообщения удалены или недоступны.{service_info}",
        "public_complete_warnings": "✅ Завершено с предупреждениями\nОбработано: {processed}/{total_posts}\nНекоторые сообщения получить не удалось.{service_info}",
        "public_complete": "✅ Завершено\nОбработано: {processed}/{total_posts}",
        "batch_flood": "⏳ Ограничение Telegram: {wait_time} с. Обработка приостановлена.\n✅ Обработано: {processed}/{total_posts}.",
        "topic_flood": "⏳ Ограничение Telegram: {wait_time} с. Обработка приостановлена.\n✅ Обработано: {processed}, ❌ ошибок: {failed}\nПовторите через {value_3} мин.",
        "need_login": "Чтобы сохранять закрытый контент, сначала подключите аккаунт Telegram через /login.",
        "banned_notice": "🚫 Вам запрещено пользоваться этим ботом.",
        "unknown_error": "Неизвестная ошибка",
        "prompt_timeout": "\n\nНа ответ есть 5 минут. Отмена: /cancel.",
        "attempts_remaining": "\n\nОсталось попыток: {count}\nОтмена: /cancel.",
        "login_limited": "⏳ Вход временно ограничен. Подождите {seconds} с и попробуйте снова. Также можно попробовать /qrlogin.",
        "logged_in": "✅ Вход выполнен",
        "logged_out": "❌ Вход не выполнен — используйте /login",
        "role_line": "\n🎭 Ваша роль: {emoji} {role}",
        "role_new_user": "Новый пользователь",
        "role_normal_user": "Пользователь",
        "role_vip_user": "VIP",
        "role_banned_user": "Заблокирован",
        "start_detail": "👋 Здравствуйте, {name}!\n\n🤖 Сохранение закрытого контента\n\n💼 Статус: {status}{role}\n\n📎 Форматы ссылок:\nhttps://t.me/c/CHANNEL_ID/POST\nhttps://t.me/c/CHANNEL_ID/101-200 (диапазон)\nhttps://t.me/c/CHANNEL_ID/101,102,103 (список)\nhttps://t.me/username/POST (публичный канал)\n\n📋 Команды:\n/login — Подключить аккаунт Telegram\n/logout — Выйти из аккаунта\n/status — Профиль и ограничения\n/stop — Остановить все операции\n/help — Полная справка\n/lang — Выбрать язык\n\nДля работы с ботом подключите аккаунт через /login.",
        "session_active": "✅ Активна",
        "session_absent": "❌ Нет",
        "none": "Нет",
        "account_regular": "Обычный",
        "profile_detail": "\n\n👤 Подключённый профиль:\nИмя: {name}\nИмя пользователя: {username}\nID: {id}\nТип: {kind}",
        "profile_error": "\n\n⚠️ Ошибка получения профиля: {error}",
        "single_only": "❌ Только один пост",
        "role_detail": "\n\n🎭 Роль: {emoji} {role}\n📊 Лимит: {count} запросов / {seconds} с\n🔗 Несколько ID/диапазон: {multi}",
        "status_detail": "📊 Ваш статус\n\n🔐 Сессия: {session}{profile}{role}{queue}\n\nℹ️ Справка: /help",
        "status_queue": "\n\n📋 Очередь: активных {active}, ожидающих {waiting}",
        "new_user_limit": "❌ Новые пользователи могут отправлять по одному посту. Для доступа к диапазонам и нескольким ID обратитесь к администратору.",
        "help_detail": "📚 Справка\n\nОтправьте ссылку Telegram, чтобы сохранить контент.\n\nОдин пост:\n`https://t.me/c/123456789/108`\n\nНесколько постов:\n`https://t.me/c/123456789/108,114,122`\n\nДиапазон постов:\n`https://t.me/c/123456789/100-110`\n\nТема форума (только сообщения этой темы):\n`https://t.me/c/123456789/5/108`\n`https://t.me/c/123456789/5/100-110`\n\nКомментарии обсуждения (все/диапазон):\n`https://t.me/c/123456789/108?thread=5`\n`https://t.me/c/123456789/108?thread=5&range10-50`\n\n/start — Начать\n/help — Справка\n/lang — Язык (English/O‘zbek/Русский)\n/login — Вход по телефону\n/qrlogin — Вход по QR-коду\n/logout — Выход\n/status — Профиль и ограничения\n/stop — Остановить все операции\n/cancel — Отменить операции\n/queue — Очередь скачивания\n/info <link> — Информация о канале/группе\n/comment <link> — Анализ комментариев\n/group <link> — Сведения о группе\n/post <link> — Сведения о посте\n/uploadsetting — Настройки отправки\n/split_media — Настройки разделения\n\nПоддерживаются фото, видео, документы, аудио, голосовые и круглые видеосообщения. Фотоальбомы группируются, смешанные медиа отправляются отдельно. Опросы и тесты включают варианты и правильные ответы; ссылки запуска QuizBot сохраняются. Большие файлы при необходимости разделяются. Для закрытых каналов сначала войдите через /login.",
        "qr_token_failed": "Не удалось создать токен QR-кода.",
        "qr_image_failed": "Не удалось создать изображение QR-кода.",
        "qr_timeout": "Время входа истекло. Попробуйте снова.",
        "qr_library_missing": "Вход по QR-коду недоступен. Используйте /login.",
        "qr_login_timeout": "Вход отменён: время ожидания истекло.",
        "qr_cancelled": "Вход отменён.",
        "qr_password_limit": "Неверный пароль. Попытки исчерпаны.",
        "qr_export_failed": "Не удалось создать сессию. Войдите снова через /login.",
        "qr_caption": "Отсканируйте QR-код в Telegram:\nНастройки → Устройства → Подключить устройство\n\nОсталось: {seconds} с\nОбновление: каждые {refresh} с\nОтмена: /cancel",
        "qr_error_detail": "Ошибка входа: {value_0}",
        "qr_2fa_error": "Ошибка входа: {value_0}",
        "queue_heading": "📋 Состояние очереди",
        "queue_current": "▶️ Сейчас: {link}",
        "queue_waiting_count": "\n📝 В очереди: {count}\n",
        "more_count": "... и ещё {count}",
        "queue_total": "\n📊 Всего обработано: {count}",
        "upload_setting_detail": "Настройка отправки: {current}\n\nauto — Без разделения только с вашей Premium-сессией\nforce_split — Всегда разделять\nno_split — Требуется ваша Premium-сессия\n\nИспользование: /uploadsetting <option>",
        "upload_invalid": "Неверный вариант: {new_setting}\nДопустимые варианты: auto, force_split, no_split",
        "upload_setting_saved": "Настройка отправки изменена: {new_setting}",
        "setting_save_failed": "Не удалось сохранить настройку. Попробуйте снова.",
        "own_premium_required": "❌ Для отключения разделения нужна ваша сессия Telegram Premium. Подключите её через /login. Иначе файлы больше 2 ГБ будут разделены.",
        "split_setting_detail": "Разделение медиа: {status}\n\n/split_media on — Всегда разделять\n/split_media off — Не разделять (нужна ваша Premium-сессия)\n/split_media auto — Автоматически",
        "split_setting_saved": "Разделение медиа: {value_0}",
        "split_invalid": "Неверный вариант: {choice}\nДопустимые варианты: on, off, auto",
        "split_mode_on": "ON ✅ — Всегда разделять подписи",
        "split_mode_off": "OFF ❌ — Не разделять (ваша Premium-сессия)",
        "split_mode_auto": "AUTO 🔄 — Автоматически",
        "upload_complete": "✅ Отправка завершена!",
        "upload_failed": "❌ Не удалось отправить",
        "eta_label": "Осталось",
        "size_label": "Размер",
        "time_label": "Время",
        "average_speed_label": "Средняя скорость",
        "calculating": "Расчёт...",
        "source_heading": "📡 Источник: {title}",
        "source_topic": "\n📂 Тема: {title}",
        "continuation_prefix": "📌 Продолжение",
        "language_invalid": "Недопустимый язык",
        "language_save_failed": "Не удалось сохранить язык. Попробуйте снова.",
        "topic_anchors": "Границы: {value_0} → {value_1}",
        "failed_ids": "\n\nНеотправленные ID:\n{id_list}",
        "deleted_count": "\n🗑 Удалено: {deleted}",
        "service_skipped": "\nℹ️ Служебных сообщений без контента: {service_skipped}",
        "message_id": "ID сообщения: {value_0}",
        "comments_range_thread": "Получение комментариев {range_start}–{range_end} из ветки {thread_id}...",
        "comments_range": "Получение комментариев {range_start}–{range_end}...",
        "comment_thread": "Получение комментария {single_comment_id} из ветки {thread_id}...",
        "comment_single": "Получение комментария {single_comment_id}...",
        "topic_processing": "Обработка темы {topic_id}...\n{status_detail}",
        "failed_more": "\n...и ещё {value_0}",
        "topic_progress": "Обработка: {value_0}/{total_posts}\nОтправлено: {processed} | Пропущено: {skipped} | Ошибок: {failed}",
        "private_stopped": "{stop_summary}\nОтправлено постов/альбомов: {processed}\nЗапрошено ID: {total_posts}\n📁 Альбомов: {albums_processed}\n❌ Ошибок: {failed}{deleted_info}{service_info}{failed_info}",
        "private_deleted": "⚠️ Все сообщения удалены\nЗапрошено: {total_posts}\n🗑 Удалено: {deleted}",
        "private_session_invalid": "Сессия недействительна. Войдите снова через /login.\nПричина: {e}",
        "download_session_invalid": "Сессия скачивания недействительна\n{e}",
        "private_none": "⚠️ Сообщения не отправлены\nЗапрошено: {total_posts}\n❌ Ошибок: {failed}{deleted_info}{service_info}{failed_info}",
        "private_complete_errors": "✅ Готово\nОтправлено: {processed}/{total_posts}\n📁 Альбомов: {albums_processed}\n❌ Ошибок: {failed}{deleted_info}{service_info}{failed_info}",
        "private_complete": "✅ Готово\nОтправлено: {processed}/{total_posts}\n📁 Альбомов: {albums_processed}{deleted_info}{service_info}",
        "premium_busy": "⚠️ Все Premium-сессии заняты ({_wait_min} мин).\nКак продолжить?",
        "batch_pause": "⏸ Пауза {pause:.0f} с... ({idx}/{total_posts})",
        "private_pause": "⏸ Пауза {pause:.0f} с... ({value_1}/{total_posts})",
        "premium_wait": "⏳ Подожду ({_wait_min} мин)",
        "premium_skip": "⚡ Продолжить без Premium",
        "batch_errors": "⚠️ Слишком много ошибок — обработка остановлена",
        "private_flood_resume": "⏳ Telegram временно ограничил запросы (FloodWait).\nТекущий ID поста: {post_id}\nОтправлено постов/альбомов: {processed}\nОжидание: {wait_time} с.\nПосле ожидания автоматически продолжу с поста {post_id}. Повторно отправлять ссылку не нужно.\nОтмена: /stop",
        "comment_analysis_detail": "Анализ комментариев\n\nКанал: {channel_title}\nID поста: {post_id}\nID ветки: {thread_id}\nГруппа обсуждения: {group_id}\nID первого комментария: {first_comment_id}\nID последнего комментария: {last_comment_id}\nВсего комментариев: {total_comments}\n\nСсылка на первый комментарий:\nhttps://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}\n\nСсылка на диапазон:\nhttps://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}&range={first_comment_id}-{last_comment_id}",
        "source_inaccessible": "⚠️ Нет доступа к исходной группе.\nID группы: {channel_id}\nАккаунт скачивания: {download_account_id}\nСравните ID группы в ссылке с группой этих тем в /userdata. Аккаунт скачивания должен состоять в группе.\nОшибка Telegram: {value_2}",
        "failure_wait": "Telegram требует временно подождать.",
        "failure_missing": "Пост удалён или не существует.",
        "failure_restricted": "Пост ограничен или отправка запрещена.",
        "failure_timeout": "Истекло время получения поста.",
        "failure_system": "При отправке поста произошла внутренняя ошибка бота.",
        "failure_generic": "Не удалось получить или отправить пост.",
        "failure_skip": "Пост пропущен.\nID поста: {post_id}\nПричина: {reason}\nПерехожу к следующему посту.",
        "session_expired_detail": "Срок действия сессии истёк\nПричина: {error}\n\n{relogin}",
        "relogin": "Войдите снова через /login.",
        "relogin_wait": "Если система свободна, повторите /login через минуту.",
    }
}


# ==================== LANGUAGE FUNCTIONS ====================

def get_user_lang(user_id: int) -> str:
    """Get user's selected language."""
    lang_file = os.path.join(LANG_DIR, f"{user_id}.txt")
    try:
        if os.path.exists(lang_file):
            with open(lang_file, 'r', encoding='utf-8') as f:
                lang = f.read().strip()
                if lang in STRINGS:
                    return lang
    except Exception:
        pass
    return DEFAULT_LANG


def set_user_lang(user_id: int, lang: str) -> bool:
    """Set user's language."""
    if lang not in STRINGS:
        return False
    
    lang_file = os.path.join(LANG_DIR, f"{user_id}.txt")
    try:
        with open(lang_file, 'w', encoding='utf-8') as f:
            f.write(lang)
        return True
    except Exception as e:
        logger.error(f"Failed to set language for {user_id}: {e}")
        return False


def get_string(user_id: int, key: str, **kwargs) -> str:
    """Get localized string for user."""
    lang = get_user_lang(user_id)
    
    # Get string from user's language
    text = STRINGS.get(lang, {}).get(key)
    
    # Fallback to English
    if text is None:
        text = STRINGS.get(DEFAULT_LANG, {}).get(key, key)
    
    # Format with kwargs
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    
    return text


def _(user_id: int, key: str, **kwargs) -> str:
    """Shortcut for get_string."""
    return get_string(user_id, key, **kwargs)


# ==================== COMMAND HANDLERS ====================

async def lang_command(client: Client, message: Message):
    """Handle /lang command."""
    user_id = message.from_user.id
    text = get_string(user_id, "lang_select")
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
            InlineKeyboardButton("🇺🇿 O'zbek", callback_data="lang_uz"),
        ],
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")],
    ])
    
    await message.reply(text, reply_markup=keyboard)


async def lang_callback(client: Client, callback: CallbackQuery):
    """Handle language selection callback."""
    user_id = callback.from_user.id
    lang = callback.data.replace("lang_", "")
    
    if lang not in STRINGS:
        await callback.answer(get_string(user_id, "language_invalid"), show_alert=True)
        return
    
    if not set_user_lang(user_id, lang):
        await callback.answer(get_string(user_id, "language_save_failed"), show_alert=True)
        return
    
    text = get_string(user_id, "lang_changed")
    
    await callback.message.edit_text(text)
    await callback.answer()


def register_lang_handlers(app: Client):
    """Register language handlers with the bot."""
    app.on_message(filters.command("lang"))(lang_command)
    app.on_callback_query(filters.regex(r"^lang_(en|uz|ru)$"))(lang_callback)
    logger.info("Language handlers registered")
