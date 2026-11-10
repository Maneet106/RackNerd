from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import os

from devgagan import app
from devgagan.core.mongo import plans_db
import pytz
import datetime
import asyncio
from config import FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID

# Global dictionaries to track payment states
payment_waiting = {}
qr_messages = {}
active_session_messages = {}  # Track messages that should NOT be deleted during active session
timeout_tasks = {}  # Track timeout tasks to cancel them when needed

async def cleanup_payment_messages(user_id: int):
    """Clean up all payment-related messages for a user"""
    try:
        messages_to_delete = []
        
        # Collect all message IDs from both tracking systems
        if user_id in active_session_messages:
            messages_to_delete.extend(active_session_messages[user_id])
        if user_id in qr_messages:
            messages_to_delete.extend(qr_messages[user_id])
        
        # Delete all collected messages
        if messages_to_delete:
            deleted_count = 0
            for msg_id in set(messages_to_delete):  # Use set to avoid duplicates
                try:
                    await app.delete_messages(user_id, msg_id)
                    deleted_count += 1
                except Exception as e:
                    pass  # Silently handle message deletion errors
            
            # Message cleanup completed silently
        
        return True
    except Exception as e:
        return False  # Silently handle cleanup errors

def clear_payment_session(user_id: int):
    """Clear payment session and cleanup message tracking"""
    try:
        # Cancel any running timeout task
        if user_id in timeout_tasks:
            timeout_tasks[user_id].cancel()
            del timeout_tasks[user_id]
        
        # Clear payment waiting state
        if user_id in payment_waiting:
            del payment_waiting[user_id]
        
        # Clear QR message tracking (but don't delete messages here)
        if user_id in qr_messages:
            del qr_messages[user_id]
        
        # Clear active session message tracking
        if user_id in active_session_messages:
            del active_session_messages[user_id]
            
        return True
    except Exception as e:
        print(f"Error clearing payment session for user {user_id}: {e}")
        return False

# Try to get User_Payment_Approve_Request from environment
try:
    User_Payment_Approve_Request = int(os.getenv("User_Payment_Approve_Request"))
except:
    User_Payment_Approve_Request = None


def build_upgrade_text() -> str:
    # Calculate multiplier dynamically from config
    multiplier = PREMIUM_LIMIT // FREEMIUM_LIMIT
    
    return (
        "<b>✨ UNLOCK PREMIUM POWER - RIDICULOUSLY CHEAP!</b>\n\n"

        "<b>🎯 PREMIUM BENEFITS (Get Instant Access!):</b>\n"
        "✅ <b>ZERO cooldowns</b> - non-stop processing\n"
        "✅ <b>Skip all queues</b> - priority processing\n"
        "✅ <b>No wait times</b> - instant access\n"
        "✅ <b>Unlimited Extraction Video/Files</b>\n"
        "✅ <b>Priority admin support</b>\n\n"

        "<b>🚀 UPGRADE TO GET MORE:</b>\n"
        f"🎁 FREE: <b>{FREEMIUM_LIMIT}</b> links + standard speed\n"
        f"✨ PREMIUM: <b>{PREMIUM_LIMIT}</b> links + ZERO cooldowns (non-stop!)\n\n"

        "⚡ <b>7 Days</b> 🌟 BEST FOR TRIAL\n"
        "   ₹100 (₹14.3/day)\n"
        "   $5.00 ($0.71/day)\n\n"
        "⭐ <b>30 Days</b> 👑 POPULAR\n"
        "   ₹200 (₹6.7/day)\n"
        "   $10.00 ($0.33/day)\n\n"
        "💎 <b>90 Days</b> 🔥 BEST VALUE\n"
        "   ₹500 (₹5.6/day)\n"
        "   $20.00 ($0.22/day)\n\n"
        "✨ <b>6 Months</b> 🎯 GREAT SAVINGS\n"
        "   ₹1000 (₹5.0/day)\n"
        "   $32.00 ($0.16/day)\n\n"
        "👑 <b>1 Year</b> ⚡ MAX SAVINGS\n"
        "   ₹1700 (₹4.7/day)\n"
        "   $45.00 ($0.12/day)\n\n"

        f"• Get <b>{multiplier}× MORE POWER</b> than others for pocket change!\n\n"

        f"<b>🔥 STOP BEING LIMITED! Others are processing {PREMIUM_LIMIT} links while you're stuck at {FREEMIUM_LIMIT} - Upgrade NOW!</b>"
    )

def build_terms_text() -> str:
    return (
        "<b>📜 PREMIUM SUBSCRIPTION TERMS & CONDITIONS</b>\n"
        "<b>AlienX Bot Premium Services</b>\n"
        "═══════════════════════════\n\n"

        "<b>✨ PREMIUM SUBSCRIPTION TERMS:</b>\n"
        "• All premium payments are <b>NON-REFUNDABLE</b>\n"
        "• Premium duration starts immediately after payment verification\n"
        "• Subscription is valid for the purchased duration only\n"
        "• No automatic renewals - manual upgrade required\n"
        "• Premium features may be modified or updated without notice\n\n"

        "<b>⭐ PREMIUM FEATURES INCLUDED:</b>\n"
        f"• Massive batch processing (up to {PREMIUM_LIMIT} links)\n"
        "• 5× faster processing speed (15 requests/minute)\n"
        "• Zero cooldowns and priority processing\n"
        "• Access to premium channels and exclusive content\n"
        "• All features subject to fair usage policy\n\n"

        "<b>💰 PAYMENT & BILLING:</b>\n"
        "• Payment must be completed within 5 minutes of session start\n"
        "• Screenshot proof required for payment verification\n"
        "• Admin approval required (typically within 1 to 24 hours)\n"
        "• Failed payments do not extend premium duration\n"
        "• Multiple payment methods supported with different fees\n\n"

        "<b>📋 LEGAL & USAGE COMPLIANCE:</b>\n"
        "• Users must respect all copyright laws and regulations\n"
        "• No copyright infringement or illegal content processing\n"
        "• Users are responsible for all content they process\n"
        "• Service may be suspended for terms violations\n"
        "• Premium benefits lost if account is suspended\n\n"

        "<b>🔧 SERVICE AVAILABILITY & MODIFICATIONS:</b>\n"
        "• We reserve the right to modify premium features\n"
        "• Service updates may temporarily affect functionality\n"
        "• No guarantee of 100% uptime or availability\n"
        "• Technical maintenance may cause temporary service interruption\n"
        "• Premium duration not extended for service downtime\n\n"

        "<b>💬 SUPPORT & DISPUTE RESOLUTION:</b>\n"
        "• Premium support provided via official channels only\n"
        "• Payment disputes handled on case-by-case basis\n"
        "• Technical issues resolved with best effort\n"
        "• Final decisions rest with service provider\n"
        "• Contact: @ZeroTrace0x for premium support\n\n"

        "<b>✅ AGREEMENT ACKNOWLEDGMENT:</b>\n"
        "<i>By proceeding with premium payment, you confirm that:</i>\n"
        "• You have read and fully understood these subscription terms\n"
        "• You agree to all conditions and policies stated above\n"
        "• You accept the non-refundable nature of premium payments\n"
        "• You understand premium features and usage limitations\n"
        "• You agree to use the service in compliance with all applicable laws\n\n"

        "<b>🏢 AlienX Bot Premium Services</b>\n"
        "<i>Professional Telegram Automation & Content Management</i>\n\n"
        
        "<b>📆 Last Updated:</b> October 2026\n"
        "<b>📧 Contact:</b> https://t.me/ZeroTrace0x for premium support"
    )


def get_upgrade_keyboard():
    """Generate upgrade keyboard with dynamic multiplier"""
    multiplier = PREMIUM_LIMIT // FREEMIUM_LIMIT
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"✨ UPGRADE NOW - GET {multiplier}× MORE POWER!", callback_data="nav:payment_methods")],
            [InlineKeyboardButton("📊 My Plan", callback_data="nav:myplan"), 
             InlineKeyboardButton("💬 Support", url="https://t.me/ZeroTrace0x")],
            [InlineKeyboardButton("📜 Terms & Conditions", callback_data="nav:terms")],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")]
        ]
    )

TERMS_KB = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("✅ I Agree - Continue to Payment", callback_data="nav:payment_methods")],
        [InlineKeyboardButton("⬅️ Back to Upgrade", callback_data="nav:open_upgrade")]
    ]
)

PAYMENT_METHODS_KB = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("💳 PAYPAL - From $0.12/day", callback_data="payment_method:cards")],
        [InlineKeyboardButton("📱 UPI (India) - Zero Fees!", callback_data="payment_method:upi")],
        [InlineKeyboardButton("🌐 Wise Transfer - 50+ Countries", callback_data="payment_method:wise")],
        [InlineKeyboardButton("₿ Crypto USDT - Anonymous", callback_data="payment_method:crypto")],
        [InlineKeyboardButton("⬅️ Back", callback_data="nav:open_upgrade")]
    ]
)

# Payment method keyboards for different plans
def get_payment_keyboard(plan_days: int, plan_price_inr: int):
    """Generate payment method keyboard based on plan"""
    
    # Calculate international prices
    usd_price = {
        7: 5.00, 30: 10.00, 90: 20.00, 200: 32.00, 365: 45.00
    }.get(plan_days, 5.00)
    
    buttons = []
    
    # International Cards - Available for all plans (Show first for global appeal)
    card_price = usd_price  # No additional fee - user pays exact amount
    buttons.append([InlineKeyboardButton(
        f"💳 Credit/Debit Card ${card_price:.2f}", 
        callback_data=f"pay:card_{plan_days}d_{card_price:.2f}"
    )])
    
    # UPI - Available for all plans (India)
    buttons.append([InlineKeyboardButton(
        f"📱 UPI (India) ₹{plan_price_inr}", 
        callback_data=f"pay:upi_{plan_days}d_{plan_price_inr}"
    )])
    
    # Crypto USDT - Available for plans $5+ (due to network fees)
    if usd_price >= 5.00:
        crypto_price = usd_price  # No additional fee - user pays exact amount
        buttons.append([InlineKeyboardButton(
            f"₿ Crypto (USDT) ${crypto_price:.2f}", 
            callback_data=f"pay:crypto_{plan_days}d_{crypto_price:.2f}"
        )])
    
    # PayPal/Coffee - Available for plans $5+ (due to minimum)
    if usd_price >= 5.00:
        coffee_price = usd_price  # No additional fee - user pays exact amount
        buttons.append([InlineKeyboardButton(
            f"🌐 PayPal/Other ${coffee_price:.2f}", 
            callback_data=f"pay:coffee_{plan_days}d_{coffee_price:.2f}"
        )])
    
    # Back button
    buttons.append([InlineKeyboardButton("⬅️ Back to Plans", callback_data="nav:open_upgrade")])
    
    return InlineKeyboardMarkup(buttons)


@app.on_message(filters.command(["upgrade", "premium"]) & filters.private)
async def upgrade_command(client, message):
    try:
        await message.reply_text(
            build_upgrade_text(),
            reply_markup=get_upgrade_keyboard(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        # Fallback without keyboard if something goes wrong
        await message.reply_text(
            build_upgrade_text(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


@app.on_callback_query(filters.regex(r"^nav:myplan$"))
async def on_nav_myplan(client, callback_query):
    try:
        uid = callback_query.from_user.id
        data = await plans_db.check_premium(uid)
        if data and data.get("expire_date"):
            expiry = data["expire_date"]
            ist = pytz.timezone("Asia/Kolkata")
            expiry_ist = expiry.astimezone(ist)
            now_ist = datetime.datetime.now(ist)
            remaining = expiry_ist - now_ist
            days = remaining.days
            hours, rem = divmod(max(remaining.seconds, 0), 3600)
            minutes, _ = divmod(rem, 60)
            text = (
                "<b>✨ Premium Status</b>\n\n"
                f"👤 User: <code>{uid}</code>\n"
                f"⏰ Expires: <code>{expiry_ist.strftime('%d-%m-%Y %I:%M:%S %p')}</code> IST\n"
                f"⏱️ Time left: <code>{days}d {hours}h {minutes}m</code>"
            )
        else:
            text = (
                "<b>🎁 You are on Free Plan</b>\n\n"
                "Upgrade to enjoy priority processing, no cooldowns, and unlimited extractions."
            )
        await callback_query.message.reply_text(text, parse_mode=ParseMode.HTML)
        await callback_query.answer()
    except Exception:
        try:
            await callback_query.answer("Unable to fetch your plan right now.")
        except Exception:
            pass


@app.on_callback_query(filters.regex(r"^nav:open_upgrade$"))
async def on_open_upgrade(client, callback_query):
    """Open the upgrade panel from start keyboard."""
    try:
        user_id = callback_query.from_user.id
        
        # Only clear session if user is not in active payment
        if user_id in payment_waiting:
            # User has active payment session - don't clear it, just navigate
            pass
        else:
            # Clean up any old QR messages from previous sessions
            if user_id in qr_messages:
                try:
                    for msg_id in qr_messages[user_id]:
                        await app.delete_messages(user_id, msg_id)
                except:
                    pass
                del qr_messages[user_id]
        
        await callback_query.message.edit_text(
            build_upgrade_text(),
            reply_markup=get_upgrade_keyboard(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer()
    except Exception:
        try:
            await callback_query.answer("Unable to open upgrade panel right now.")
        except Exception:
            pass

@app.on_callback_query(filters.regex(r"^nav:terms$"))
async def on_terms(client, callback_query):
    try:
        user_id = callback_query.from_user.id
        
        # Only clear session if user is not in active payment
        if user_id in payment_waiting:
            # User has active payment session - don't clear it, just navigate
            pass
        else:
            # Clean up any old QR messages from previous sessions
            if user_id in qr_messages:
                try:
                    for msg_id in qr_messages[user_id]:
                        await app.delete_messages(user_id, msg_id)
                except:
                    pass
                del qr_messages[user_id]
        
        await callback_query.message.edit_text(
            build_terms_text(),
            reply_markup=TERMS_KB,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer()
    except Exception as e:
        print(f"Error loading terms: {e}")
        await callback_query.answer("Unable to load terms. Please try again.")

@app.on_callback_query(filters.regex(r"^nav:payment_methods$"))
async def on_payment_methods(client, callback_query):
    try:
        user_id = callback_query.from_user.id
        
        # Only clear session if user is not in active payment
        if user_id in payment_waiting:
            # User has active payment session - don't clear it, just navigate
            pass
        # DUPLICATE CODE COMMENTED OUT - Same cleanup logic exists in on_open_upgrade
        else:
            # Clean up any old QR messages from previous sessions
            if user_id in qr_messages:
                try:
                    for msg_id in qr_messages[user_id]:
                        await app.delete_messages(user_id, msg_id)
                except:
                    pass
                del qr_messages[user_id]
        
        text = (
            "<b>◈ PAYMENT CENTER</b>\n"
            "<i>⟡ Choose your preferred payment method</i>\n\n"
            
            
            "<b>🔐 Secure Payment Processing</b>\n"
            "• All transactions are encrypted\n"
            "• Multiple payment gateways supported\n"
            "• Instant activation after payment\n"
            "• QR codes provided for easy payment\n\n"
            
            "<i>Select a payment method to view available plans and pricing.</i>"
        )
        
        await callback_query.message.edit_text(
            text,
            reply_markup=PAYMENT_METHODS_KB,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await callback_query.answer()
    except Exception:
        await callback_query.answer("Unable to load payment methods.")


# Payment method selection handlers
@app.on_callback_query(filters.regex(r"^payment_method:(cards|upi|wise|crypto)$"))
async def on_payment_method_selected(client, callback_query):
    """Handle payment method selection and show plans"""
    try:
        user_id = callback_query.from_user.id
        method = callback_query.data.split(":")[1]
        
        # CRITICAL: Clear any existing payment session when selecting payment method
        if user_id in payment_waiting:
            # Clean up ALL existing session messages BEFORE clearing session
            await cleanup_payment_messages(user_id)
            
            # Now clear the session (this will clear tracking dictionaries)
            clear_payment_session(user_id)
        
        if method == "cards":
            text = (
                "<b>💳 CREDIT/DEBIT CARDS/BANK TRANSFER</b>\n"
                "<b>Powered by PayPal</b>\n\n"
                
                "<b>💰 INSANE PRICES (Cheaper than snacks!):</b>\n\n"
                
                "⚡ <b>7 Days</b> - $5.00 ($0.71/day)\n"
                "⭐ <b>30 Days</b> - $10.00 ($0.33/day) 👑 POPULAR\n"
                "💎 <b>90 Days</b> - $20.00 ($0.22/day) 🔥 BEST VALUE\n"
                "✨ <b>6 Months</b> - $32.00 ($0.16/day)\n"
                "👑 <b>1 Year</b> - $45.00 ($0.12/day) ⚡ MAX SAVINGS\n\n"
                
                "<b>⚠️ PAYMENT FEES NOTICE:</b>\n"
                "<b>Pay exactly the shown amount covering transaction fees</b>\n"
                "<i>Else shortfall will happen and duration will be deducted from subscription</i>\n\n"
                
                "<b>💳 Accepted Cards:</b>\n"
                "• Visa, MasterCard, American Express\n"
                "• Debit cards with international usage\n"
                "• Digital wallets (Apple Pay, Google Pay)\n\n"
                
                "<b>⏰ IMPORTANT: You have only 5 minutes after selecting a plan!</b>\n"
                "<i>Session expires automatically. If expired while paying, start new session and send screenshot - admin will approve manually.</i>\n\n"
                
                "<b>👇 Select your plan duration below:</b>"
            )
            
            # Send new message with logo
            try:
                user_id = callback_query.from_user.id
                
                # DUPLICATE CODE COMMENTED OUT - QR cleanup handled in main session clearing logic
                # Clean up any existing QR messages for this user
                if user_id in qr_messages:
                    try:
                        for msg_id in qr_messages[user_id]:
                            await app.delete_messages(user_id, msg_id)
                    except:
                        pass
                    del qr_messages[user_id]
                
                logo_msg = await callback_query.message.reply_photo(
                    photo=open("payment_images/paypal_logo.jpg", "rb"),
                    caption=text,
                    parse_mode=ParseMode.HTML
                )
                
                # Track logo message for cleanup
                qr_messages[user_id] = [logo_msg.id]
                
            except Exception as e:
                print(f"Error sending PayPal logo: {e}")
                # Fallback to text message
                await callback_query.message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            
        elif method == "upi":
            text = (
                "<b>📱 UPI PAYMENT (INDIA)</b>\n"
                "<b>Instant Transfer - Zero Fees</b>\n\n"
                
                "<b>💰 INSANE PRICES (Cheaper than chai!):</b>\n\n"

                "⚡ <b>7 Days</b> - ₹100 (₹14.3/day)\n"
                "⭐ <b>30 Days</b> - ₹200 (₹6.7/day) 👑 POPULAR\n"
                "💎 <b>90 Days</b> - ₹500 (₹5.6/day) 🔥 BEST VALUE\n"
                "✨ <b>6 Months</b> - ₹1000 (₹5.0/day)\n"
                "👑 <b>1 Year</b> - ₹1700 (₹4.7/day) ⚡ MAX SAVINGS\n\n"
                
                "<b>✅ PAYMENT FEES NOTICE:</b>\n"
                "<b>Pay Exact Amount - Zero Extra Fees Required!</b>\n"
                "<i>UPI transfers are completely free with no hidden charges</i>\n\n"
                
                "<b>📱 Supported Apps:</b>\n"
                "• PhonePe, Google Pay, Paytm\n"
                "• BHIM, Amazon Pay, WhatsApp Pay\n"
                "• Any UPI-enabled banking app\n\n"
                
                "<b>⏰ IMPORTANT: You have only 5 minutes after selecting a plan!</b>\n"
                "<i>Session expires automatically. If expired while paying, start new session and send screenshot - admin will approve manually.</i>\n\n"
                
                "<b>👇 Select your plan duration below:</b>"
            )
            
            # Send new message with logo
            try:
                user_id = callback_query.from_user.id
                
                # DUPLICATE CODE COMMENTED OUT - QR cleanup handled in main session clearing logic
                # Clean up any existing QR messages for this user
                if user_id in qr_messages:
                    try:
                        for msg_id in qr_messages[user_id]:
                            await app.delete_messages(user_id, msg_id)
                    except:
                        pass
                    del qr_messages[user_id]
                
                logo_msg = await callback_query.message.reply_photo(
                    photo=open("payment_images/upi_logo.jpg", "rb"),
                    caption=text,
                    parse_mode=ParseMode.HTML
                )
                
                # Track logo message for cleanup
                qr_messages[user_id] = [logo_msg.id]
                
            except Exception as e:
                print(f"Error sending UPI logo: {e}")
                # Fallback to text message
                await callback_query.message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            
        elif method == "wise":
            text = (
                "<b>🌐 WISE TRANSFER</b>\n"
                "<b>International Bank Transfers</b>\n\n"
                
                "<b>💰 INSANE PRICES (Cheaper than coffee!):</b>\n"

                "⚡ <b>7 Days</b> - $5.00 ($0.71/day)\n"
                "⭐ <b>30 Days</b> - $10.00 ($0.33/day) 👑 POPULAR\n"
                "💎 <b>90 Days</b> - $20.00 ($0.22/day) 🔥 BEST VALUE\n"
                "✨ <b>6 Months</b> - $32.00 ($0.16/day)\n"
                "👑 <b>1 Year</b> - $45.00 ($0.12/day) ⚡ MAX SAVINGS\n\n"
                
                "<b>⚠️ PAYMENT FEES NOTICE:</b>\n"
                "<b>Pay exactly the shown amount covering transfer fees</b>\n"
                "<i>Else shortfall will happen and duration will be deducted from subscription</i>\n\n"
                
                "<b>🌍 Supported Countries:</b>\n"
                "• USA, UK, Canada, Australia\n"
                "• European Union countries\n"
                "• 50+ countries worldwide\n\n"
                
                "<b>⏰ IMPORTANT: You have only 5 minutes after selecting a plan!</b>\n"
                "<i>Session expires automatically. If expired while paying, start new session and send screenshot - admin will approve manually.</i>\n\n"
                
                "<b>👇 Select your plan duration below:</b>"
            )
            
            # Send new message with logo
            try:
                user_id = callback_query.from_user.id
                
                # DUPLICATE CODE COMMENTED OUT - QR cleanup handled in main session clearing logic
                # Clean up any existing QR messages for this user
                if user_id in qr_messages:
                    try:
                        for msg_id in qr_messages[user_id]:
                            await app.delete_messages(user_id, msg_id)
                    except:
                        pass
                    del qr_messages[user_id]
                
                logo_msg = await callback_query.message.reply_photo(
                    photo=open("payment_images/wise_logo.png", "rb"),
                    caption=text,
                    parse_mode=ParseMode.HTML
                )
                
                # Track logo message for cleanup
                qr_messages[user_id] = [logo_msg.id]
                
            except Exception as e:
                print(f"Error sending Wise logo: {e}")
                # Fallback to text message
                await callback_query.message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            
        elif method == "crypto":
            text = (
                "<b>₿ CRYPTOCURRENCY</b>\n\n"
                
                "⚡ <b>7 Days</b> - $5.00 USDT ($0.71/day)\n"
                "⭐ <b>30 Days</b> - $10.00 USDT ($0.33/day) 👑 POPULAR\n"
                "💎 <b>90 Days</b> - $20.00 USDT ($0.22/day) 🔥 BEST VALUE\n"
                "✨ <b>6 Months</b> - $32.00 USDT ($0.16/day)\n"
                "👑 <b>1 Year</b> - $45.00 USDT ($0.12/day) ⚡ MAX SAVINGS\n\n"
                
                "<b>⚠️ PAYMENT FEES NOTICE:</b>\n"
                "<b>Pay exactly the shown amount covering network fees</b>\n"
                "<i>Else shortfall will happen and duration will be deducted from subscription</i>\n\n"
                
               
                "• Other crypto: Contact admin @zerotrace0x\n\n"
                
                "<b>⏰ IMPORTANT: You have only 5 minutes after selecting a plan!</b>\n"
                "<i>Session expires automatically. If expired while paying, start new session and send screenshot - admin will approve manually.</i>\n\n"
                
                "<b>👇 Select your plan duration below:</b>"
            )
            
            # Send new message with logo
            try:
                user_id = callback_query.from_user.id
                
                # DUPLICATE CODE COMMENTED OUT - QR cleanup handled in main session clearing logic
                # Clean up any existing QR messages for this user
                if user_id in qr_messages:
                    try:
                        for msg_id in qr_messages[user_id]:
                            await app.delete_messages(user_id, msg_id)
                    except:
                        pass
                    del qr_messages[user_id]
                
                logo_msg = await callback_query.message.reply_photo(
                    photo=open("payment_images/cryptocurrency_logo.jpg", "rb"),
                    caption=text,
                    parse_mode=ParseMode.HTML
                )
                
                # Track logo message for cleanup
                qr_messages[user_id] = [logo_msg.id]
                
            except Exception as e:
                print(f"Error sending Crypto logo: {e}")
                # Fallback to text message
                await callback_query.message.reply_text(text, parse_mode=ParseMode.HTML, disable_we12_page_preview=True)
        
        # Create plan selection keyboard for the chosen method in 2x2 format
        plan_buttons = []
        if method == "cards":
            # Cards: Original prices (user covers fees) - 2x2 + 1x1 layout
            plan_buttons = [
                [InlineKeyboardButton("⚡ 7 Days - $5.00", callback_data=f"buy:{method}_7d_5.00"),
                 InlineKeyboardButton("⭐ 30 Days - $10.00", callback_data=f"buy:{method}_30d_10.00")],
                [InlineKeyboardButton("💎 90 Days - $20.00", callback_data=f"buy:{method}_90d_20.00"),
                 InlineKeyboardButton("✨ 6 Months - $32.00", callback_data=f"buy:{method}_200d_32.00")],
                [InlineKeyboardButton("👑 1 Year - $45.00 ⚡ MAX SAVINGS", callback_data=f"buy:{method}_365d_45.00")]
            ]
        elif method == "upi":
            # UPI: Strategic pricing for Indian market (no extra fees) - 2x2 + 1x1 layout
            plan_buttons = [
                [InlineKeyboardButton("⚡ 7 Days - ₹100", callback_data=f"buy:{method}_7d_100"),
                 InlineKeyboardButton("⭐ 30 Days - ₹200", callback_data=f"buy:{method}_30d_200")],
                [InlineKeyboardButton("💎 90 Days - ₹500", callback_data=f"buy:{method}_90d_500"),
                 InlineKeyboardButton("✨ 6 Months - ₹1000", callback_data=f"buy:{method}_200d_1000")],
                [InlineKeyboardButton("👑 1 Year - ₹1700 ⚡ MAX SAVINGS", callback_data=f"buy:{method}_365d_1700")]
            ]
        elif method == "wise":
            # Wise: Original prices (user covers fees) - 2x2 + 1x1 layout
            plan_buttons = [
                [InlineKeyboardButton("⚡ 7 Days - $5.00", callback_data=f"buy:{method}_7d_5.00"),
                 InlineKeyboardButton("⭐ 30 Days - $10.00", callback_data=f"buy:{method}_30d_10.00")],
                [InlineKeyboardButton("💎 90 Days - $20.00", callback_data=f"buy:{method}_90d_20.00"),
                 InlineKeyboardButton("✨ 6 Months - $32.00", callback_data=f"buy:{method}_200d_32.00")],
                [InlineKeyboardButton("👑 1 Year - $45.00 ⚡ MAX SAVINGS", callback_data=f"buy:{method}_365d_45.00")]
            ]
        elif method == "crypto":
            # Crypto: Original prices (user covers fees) - 2x2 + 1x1 layout
            plan_buttons = [
                [InlineKeyboardButton("⚡ 7 Days - $5.00", callback_data=f"buy:{method}_7d_5.00"),
                 InlineKeyboardButton("⭐ 30 Days - $10.00", callback_data=f"buy:{method}_30d_10.00")],
                [InlineKeyboardButton("💎 90 Days - $20.00", callback_data=f"buy:{method}_90d_20.00"),
                 InlineKeyboardButton("✨ 6 Months - $32.00", callback_data=f"buy:{method}_200d_32.00")],
                [InlineKeyboardButton("👑 1 Year - $45.00 ⚡ MAX SAVINGS", callback_data=f"buy:{method}_365d_45.00")]
            ]
        
        plan_buttons.append([InlineKeyboardButton("❌ Cancel Payment Session", callback_data="cancel_payment")])
        
        # Add keyboard to the logo message if it exists
        user_id = callback_query.from_user.id
        if user_id in qr_messages and qr_messages[user_id]:
            try:
                # Edit the logo message to add the keyboard
                await app.edit_message_reply_markup(
                    user_id, 
                    qr_messages[user_id][0], 
                    reply_markup=InlineKeyboardMarkup(plan_buttons)
                )
            except Exception as e:
                print(f"Error adding keyboard to logo message: {e}")
                # Fallback: send keyboard as separate message
                await callback_query.message.reply_text(
                    "👇 <b>Select your plan:</b>",
                    reply_markup=InlineKeyboardMarkup(plan_buttons),
                    parse_mode=ParseMode.HTML
                )
        
        await callback_query.answer()
        
    except Exception as e:
        await callback_query.answer("Error loading payment method.")

# Plan purchase handlers
@app.on_callback_query(filters.regex(r"^buy:(cards|upi|wise|crypto)_(\d+)d_(.+)$"))
async def on_plan_purchase(client, callback_query):
    """Handle plan purchase and initiate payment process"""
    try:
        # Parse callback data
        parts = callback_query.data.split(":")
        payment_info = parts[1]  # e.g., "cards_7d_1.50"
        
        method_and_plan = payment_info.split("_")
        method = method_and_plan[0]
        days = int(method_and_plan[1].replace("d", ""))
        price = method_and_plan[2]
        
        user_id = callback_query.from_user.id
        user = callback_query.from_user
        
        # CRITICAL: Clear any existing payment session before starting new one
        if user_id in payment_waiting:
            # Clean up ALL existing session messages BEFORE clearing session
            await cleanup_payment_messages(user_id)
            
            # Now clear the session (this will clear tracking dictionaries)
            clear_payment_session(user_id)
        
        # Plan names
        plan_names = {7: "7-Day Starter", 30: "30-Day Popular", 90: "90-Day Pro", 200: "6-Month Premium", 365: "1-Year Ultimate"}
        plan_name = plan_names.get(days, f"{days}-Day Plan")
        
        # Generate single comprehensive message based on method
        if method == "cards":
            # Send single message with QR code and all instructions
            try:
                payment_msg = await callback_query.message.reply_photo(
                    photo=open("payment_images/paypal_qr.jpg", "rb"),
                    caption=f"<b>💳PAYPAL - {plan_name}</b>\n"
                           f"💰 <b>Total Amount:</b> ${price}\n"
                           f"⚠️ <b>Pay exactly this amount</b> covering transaction fees\n\n"
                           
                           f"<b>🔗 Quick Payment:</b>\n"
                           f"• Click: <a href='https://paypal.me/ManeetJassal214'>PayPal</a>\n"
                           f"• Pay to: ManeetJassal214\n"
                           f"• Pay <b>${price}</b> to avoid duration deduction\n\n"
                           
                           f"<b>📸 Next Steps:</b>\n"
                           f"1️⃣ Complete payment (exact amount)\n"
                           f"2️⃣ Send screenshot here\n\n"
                           
                           f"🔔 <b>SESSION ACTIVE:</b> {plan_name}\n"
                           f"⏰ <b>Time:</b> 5 minutes remaining\n",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel Payment Session", callback_data="cancel_payment")]
                    ])
                )
                
                # Initialize tracking lists if they don't exist
                if user_id not in active_session_messages:
                    active_session_messages[user_id] = []
                if user_id not in qr_messages:
                    qr_messages[user_id] = []
                
                # Track this single message
                active_session_messages[user_id].append(payment_msg.id)
                qr_messages[user_id].append(payment_msg.id)
                    
            except Exception as e:
                print(f"Error sending Cards payment: {e}")
                # UNUSED VARIABLE COMMENTED OUT - Fallback text not used anywhere
                # # Fallback to text message
                # payment_text = f"<b>💳 PAYPAL - {plan_name}</b>\nAmount: ${price}\nLink: https://paypal.me/ManeetJassal214"
            
        elif method == "upi":
            # Send single message with QR code and all instructions
            try:
                payment_msg = await callback_query.message.reply_photo(
                    photo=open("payment_images/upi_qr.jpg", "rb"),
                    caption=f"<b>📱 UPI PAYMENT - {plan_name}</b>\n"
                           f"💰 <b>Exact Amount:</b> ₹{price}\n"
                           f"✅ <b>Zero Extra Fees!</b> Pay exact amount only\n\n"
                           
                           f"<b>💳 Payment Details:</b>\n"
                           f"UPI ID: <code>alienxjassal@fam</code>\n"
                           f"Name: Maneet Singh Jassal\n\n"
                           
                           f"<b>📱 Quick Payment:</b>\n"
                           f"• Scan QR code above\n"
                           f"• Or use UPI ID directly\n"
                           f"• Pay exactly <b>₹{price}</b> (no extra fees needed)\n\n"
                           
                           f"<b>📸 Next Steps:</b>\n"
                           f"1️⃣ Complete payment\n"
                           f"2️⃣ Screenshot confirmation\n"
                           f"3️⃣ Send screenshot here\n\n"
                           
                           f"🔔 <b>SESSION ACTIVE:</b> {plan_name}\n"
                           f"⏰ <b>Time:</b> 5 minutes remaining\n"
                           f"⚡ <b>Zero fees • Instant transfer</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel Payment Session", callback_data="cancel_payment")]
                    ])
                )
                
                # DUPLICATE CODE COMMENTED OUT - Tracking initialization handled at end of function
                # Initialize tracking lists if they don't exist
                if user_id not in active_session_messages:
                    active_session_messages[user_id] = []
                if user_id not in qr_messages:
                    qr_messages[user_id] = []
                
                # Track this single message
                active_session_messages[user_id].append(payment_msg.id)
                qr_messages[user_id].append(payment_msg.id)
                    
            except Exception as e:
                print(f"Error sending UPI payment: {e}")
            
        elif method == "wise":
            # Send single message with QR code and all instructions
            try:
                payment_msg = await callback_query.message.reply_photo(
                    photo=open("payment_images/wise_qr.jpg", "rb"),
                    caption=f"<b>🌐 WISE TRANSFER - {plan_name}</b>\n"
                           f"💰 <b>Total Amount:</b> ${price}\n"
                           f"⚠️ <b>Pay exactly this amount</b> covering transfer fees\n\n"
                           
                           f"<b>🔗 Quick Payment:</b>\n"
                           f"• Click: <a href='https://wise.com/pay/business/maneetsinghjassal'>Wise Payment</a>\n"
                           f"• Or scan QR code above\n"
                           f"• Pay to: maneetsinghjassal\n"
                           f"• Pay <b>${price}</b> to avoid duration deduction\n\n"
                           
                           f"<b>📸 Next Steps:</b>\n"
                           f"1️⃣ Complete payment (exact amount)\n"
                           f"2️⃣ Send screenshot here\n\n"
                           
                           f"🔔 <b>SESSION ACTIVE:</b> {plan_name}\n"
                           f"⏰ <b>Time:</b> 5 minutes remaining\n"
                           f"🌍 <b>Available in 50+ countries</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel Payment Session", callback_data="cancel_payment")]
                    ])
                )
                
                # DUPLICATE CODE COMMENTED OUT - Tracking initialization handled at end of function
                # Initialize tracking lists if they don't exist
                if user_id not in active_session_messages:
                    active_session_messages[user_id] = []
                if user_id not in qr_messages:
                    qr_messages[user_id] = []
                
                # Track this single message
                active_session_messages[user_id].append(payment_msg.id)
                qr_messages[user_id].append(payment_msg.id)
                    
            except Exception as e:
                print(f"Error sending Wise payment: {e}")
                
        elif method == "crypto":
            # Send single message with Binance QR code and all instructions
            try:
                payment_msg = await callback_query.message.reply_photo(
                    photo=open("payment_images/binance_qr.jpg", "rb"),
                    caption=f"<b>₿ CRYPTO - {plan_name}</b>\n"
                           f"💰 <b>Total Amount:</b> ${price} USDT\n"
                           f"⚠️ <b>Pay exactly this amount</b> covering network fees\n\n"
                           
                           f"<b>🔗 Wallet Addresses:</b>\n"
                           f"<b>USDT TRC20:</b> <code>TWxsdndK7QugJfYMvL8ETWVfJNPhbu1rAV</code>\n\n"

                           f"<b>USDT BEP20:</b> <code>0x39875849d457081aaa409ea2a9360778789af3bf</code>\n\n"

                           f"<b>Binance Pay ID:</b> <code>1035380595</code>\n\n"
                           
                           f"<b>💱 Want to pay in BTC, ETH, or other crypto?</b>\n"
                           f"• Contact admin: <a href='https://t.me/zerotrace0x'>@zerotrace0x</a>\n\n"
                           
                           f"<b>📸 Next Steps:</b>\n"
                           f"1️⃣ Complete payment (exact amount)\n"
                           f"2️⃣ Send screenshot here\n\n"
                           
                           f"🔔 <b>SESSION ACTIVE:</b> {plan_name}\n"
                           f"⏰ <b>Time:</b> 5 minutes remaining\n",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel Payment Session", callback_data="cancel_payment")]
                    ])
                )
                
                # DUPLICATE CODE COMMENTED OUT - Tracking initialization handled at end of function
                # Initialize tracking lists if they don't exist
                if user_id not in active_session_messages:
                    active_session_messages[user_id] = []
                if user_id not in qr_messages:
                    qr_messages[user_id] = []
                
                # Track this single message
                active_session_messages[user_id].append(payment_msg.id)
                qr_messages[user_id].append(payment_msg.id)
                    
            except Exception as e:
                print(f"Error sending Crypto payment: {e}")
        
        # Store payment waiting state with proper timezone
        ist = pytz.timezone("Asia/Kolkata")
        current_time = datetime.datetime.now(ist)
        
        # Calculate the actual price based on method and plan
        if method == "upi":
            # UPI prices in INR
            upi_prices = {7: 100, 30: 200, 90: 500, 200: 1000, 365: 1700}
            actual_price = upi_prices.get(days, 100)
        else:
            # International prices in USD
            usd_prices = {7: 5.00, 30: 10.00, 90: 20.00, 200: 32.00, 365: 45.00}
            actual_price = usd_prices.get(days, 5.00)
        
        payment_waiting[user_id] = {
            "method": method,
            "days": days,
            "price": actual_price,  # Use calculated price, not callback data
            "timestamp": current_time,
            "plan_name": plan_name,
            "active": True
        }
        
        # Delete the original upgrade message to prevent confusion
        try:
            await callback_query.message.delete()
        except:
            pass
        
        # Clean up any existing OLD QR messages (only non-active session messages)
        if user_id in qr_messages:
            try:
                for msg_id in qr_messages[user_id]:
                    # Only delete if it's not in active session messages
                    if user_id not in active_session_messages or msg_id not in active_session_messages[user_id]:
                        await app.delete_messages(user_id, msg_id)
            except:
                pass
        
        # Initialize tracking if not already done (for methods that send their own messages)
        if user_id not in active_session_messages:
            active_session_messages[user_id] = []
        if user_id not in qr_messages:
            qr_messages[user_id] = []
        
        await callback_query.answer("Payment session started! Send screenshot within 5 minutes.")
        
        # Start 5-minute timer with proper error handling and track the task
        timeout_task = asyncio.create_task(payment_timeout(user_id, current_time))
        timeout_tasks[user_id] = timeout_task
        
    except Exception as e:
        await callback_query.answer("Error processing payment. Try again.")

# Cancel payment session handler
@app.on_callback_query(filters.regex(r"^cancel_payment$"))
async def cancel_payment_session(client, callback_query):
    """Handle manual payment session cancellation"""
    try:
        user_id = callback_query.from_user.id
        
        # Clean up ALL existing session messages BEFORE clearing session
        await cleanup_payment_messages(user_id)
        
        # Now clear the session (this will clear tracking dictionaries and cancel timeout task)
        clear_payment_session(user_id)
        
        # Delete the current message silently
        try:
            await callback_query.message.delete()
        except:
            pass
        
        # Silent cancellation - no message sent to save API calls and keep chat clean
        await callback_query.answer()
        
    except Exception as e:
        print(f"Error cancelling payment session: {e}")
        await callback_query.answer("Error cancelling session. Please try /upgrade again.")


async def payment_timeout(user_id: int, start_time):
    """Handle payment timeout after 5 minutes"""
    try:
        await asyncio.sleep(300)  # 5 minutes = 300 seconds
        
        # Check if user is still waiting and session is active
        if user_id in payment_waiting:
            payment_info = payment_waiting[user_id]
            
            # Double-check timing to prevent premature timeout
            ist = pytz.timezone("Asia/Kolkata")
            current_time = datetime.datetime.now(ist)
            elapsed = (current_time - start_time).total_seconds()
            
            # CRITICAL: Check if this timeout belongs to the current session
            session_start_time = payment_info.get("timestamp")
            if session_start_time and session_start_time != start_time:
                return  # This timeout is for an old session, ignore it
            
            # Only timeout if actually 5+ minutes have passed and still active
            if elapsed >= 300 and payment_info.get("active", True):
                # Clean up QR messages for this user
                if user_id in qr_messages:
                    try:
                        for msg_id in qr_messages[user_id]:
                            await app.delete_messages(user_id, msg_id)
                    except:
                        pass
                    del qr_messages[user_id]
                
                # Remove from waiting list and cleanup task tracking
                del payment_waiting[user_id]
                if user_id in timeout_tasks:
                    del timeout_tasks[user_id]
                
                try:
                    await app.send_message(
                        user_id,
                        "⏰ <b>Payment Session Expired!</b>\n\n"
                        "🕐 <b>Time's up!</b> You didn't send the payment screenshot within 5 minutes.\n\n"
                        "💡 <b>What to do now?</b>\n"
                        "• If you already paid: Start a new session with /upgrade and send your payment screenshot\n"
                        "• If you haven't paid yet: Start fresh with /upgrade\n\n"
                        "🔄 <b>Admin will manually approve if you already paid!</b>\n"
                        "⏱️ <i>Manual approval may take time when admin comes online</i>",
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
    except Exception:
        # Silently handle any errors in timeout function
        pass

# Handle payment screenshots
@app.on_message(filters.photo & filters.private)
async def handle_payment_screenshot(client, message):
    """Handle payment screenshot from users"""
    user_id = message.from_user.id
    
    if user_id not in payment_waiting:
        return  # Not waiting for payment from this user
    
    try:
        payment_info = payment_waiting[user_id]
        
        # Check if session is still active
        if not payment_info.get("active", True):
            return  # Session already processed or expired
        
        # Cancel the timeout task since we received the screenshot
        if user_id in timeout_tasks:
            timeout_tasks[user_id].cancel()
            del timeout_tasks[user_id]
        
        user = message.from_user
        
        # Clean up QR messages for this user
        if user_id in qr_messages:
            try:
                for msg_id in qr_messages[user_id]:
                    await app.delete_messages(user_id, msg_id)
            except:
                pass
            del qr_messages[user_id]
        
        # Mark session as inactive and remove from waiting list
        payment_info["active"] = False
        del payment_waiting[user_id]
        
        # Prepare admin notification
        admin_text = (
            "💰 <b>PAYMENT SCREENSHOT RECEIVED</b>\n\n"
            f"👤 User: {user.first_name or 'Unknown'}\n"
            f"🆔 User ID: <code>{user_id}</code>\n"
            f"📞 Username: @{user.username or 'None'}\n"
            f"📱 Name: {user.first_name or ''} {user.last_name or ''}\n\n"
            
            f"💳 Payment Method: <b>{payment_info['method'].upper()}</b>\n"
            f"📦 Plan: <b>{payment_info['plan_name']}</b>\n"
            f"💰 Amount: <b>{payment_info['price']}</b>\n"
            f"⏰ Duration: <b>{payment_info['days']} days</b>\n\n"
            
            f"<b>⚡ Action Required:</b>\n"
            f"Verify payment and activate premium for user {user_id}"
        )
        
        # Send to payment approval group if configured, otherwise fallback to owner
        if User_Payment_Approve_Request:
            try:
                await message.forward(User_Payment_Approve_Request)
                await app.send_message(User_Payment_Approve_Request, admin_text, parse_mode=ParseMode.HTML)
            except Exception:
                # Fallback to owner if group fails
                owner_id = OWNER_ID[0] if isinstance(OWNER_ID, list) and OWNER_ID else OWNER_ID
                await message.forward(owner_id)
                await app.send_message(owner_id, admin_text, parse_mode=ParseMode.HTML)
        else:
            # Send to owner if no group configured
            owner_id = OWNER_ID[0] if isinstance(OWNER_ID, list) and OWNER_ID else OWNER_ID
            await message.forward(owner_id)
            await app.send_message(owner_id, admin_text, parse_mode=ParseMode.HTML)
        
        # Confirm to user
        await message.reply_text(
            "✅ <b>Payment screenshot received!</b>\n\n"
            "Your payment is being verified by our admin.\n"
            "You'll receive confirmation within 1 to 24 hours.\n\n"
            "Thank you for upgrading to premium! 🚀\n\n"
            "Only Contact Admin After 6 Hours of Unverified Payment: @ZeroTrace0x",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        await message.reply_text(
            "❌ Error processing your screenshot.\n"
            "Please contact admin: @ZeroTrace0x",
            parse_mode=ParseMode.HTML
        )

# Global handler to clear payment sessions when users use other commands
@app.on_message(filters.command(["start", "help", "batch", "login", "logout", "myplan", "cancel", "upgrade", "settings", "stats", "info"]) & filters.private)
async def clear_session_on_command(client, message):
    """Clear payment session when user uses other commands"""
    user_id = message.from_user.id
    command = message.command[0] if message.command else "unknown"
    
    # Only clear if user has an active payment session
    if user_id in payment_waiting:
        # Clear the session
        clear_payment_session(user_id)
        
        # Clean up ALL session messages (QR codes, payment instructions, etc.)
        if user_id in active_session_messages:
            try:
                for msg_id in active_session_messages[user_id]:
                    await app.delete_messages(user_id, msg_id)
            except:
                pass
            del active_session_messages[user_id]
        
        # Also clean up QR message tracking
        if user_id in qr_messages:
            del qr_messages[user_id]
        
        # Notify user that payment session was cancelled
        try:
            await message.reply_text(
                "⚠️ <b>Payment session cancelled!</b>\n\n"
                "You started using other commands, so your payment session has been automatically cancelled.\n"
                "All upgrade messages have been cleaned up.\n\n"
                "💡 <b>To upgrade again:</b> Use /upgrade or /start → Upgrade to Premium",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💎 Start New Upgrade", callback_data="nav:open_upgrade")]
                ]),
                parse_mode=ParseMode.HTML
            )
        except:
            pass

# Handler for any text messages (non-commands) that should clear sessions
@app.on_message(filters.text & filters.private & ~filters.command(["upgrade"]))
async def clear_session_on_text(client, message):
    """Clear payment session when user sends any text (except upgrade command)"""
    user_id = message.from_user.id
    
    # Only clear if user has an active payment session and it's not a screenshot-related message
    if user_id in payment_waiting and not any(keyword in message.text.lower() for keyword in ["payment", "screenshot", "paid", "done"]):
        clear_payment_session(user_id)
        
        # Clean up ALL session messages
        if user_id in active_session_messages:
            try:
                for msg_id in active_session_messages[user_id]:
                    await app.delete_messages(user_id, msg_id)
            except:
                pass
            del active_session_messages[user_id]
        
        # Also clean up QR message tracking
        if user_id in qr_messages:
            del qr_messages[user_id]
        
        # Notify user that payment session was cancelled
        try:
            await message.reply_text(
                "⚠️ <b>Payment session cancelled!</b>\n\n"
                "You started using other features, so your payment session has been automatically cancelled.\n"
                "All upgrade messages have been cleaned up.\n\n"
                "💡 <b>To upgrade again:</b> Use /upgrade or /start → Upgrade to Premium",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💎 Start New Upgrade", callback_data="nav:open_upgrade")]
                ]),
                parse_mode=ParseMode.HTML
            )
        except:
            pass
