import os, re, subprocess, sys, traceback
from io import StringIO
from time import time
from pyrogram import filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from config import OWNER_ID
from devgagan import app

async def aexec(code, client, message):
    # Create a proper execution environment with common modules
    exec_globals = {
        '__builtins__': __builtins__,
        'client': client,
        'message': message,
        'app': client,
    }
    
    # Import commonly used modules
    try:
        import os, sys, asyncio, time, datetime, json, re, textwrap
        exec_globals.update({
            'os': os,
            'sys': sys,
            'asyncio': asyncio,
            'time': time,
            'datetime': datetime,
            'json': json,
            're': re,
            'textwrap': textwrap,
            'print': print,
        })
    except:
        pass
    
    # Use textwrap.dedent to normalize indentation
    import textwrap
    
    # Strip and dedent the code
    code_clean = code.strip()
    dedented = textwrap.dedent(code_clean)
    
    # Build the async function with proper indentation
    lines = dedented.split('\n')
    exec_code = "async def __aexec(client, message):\n"
    
    for line in lines:
        # Add 4 spaces to every line (including empty ones to preserve structure)
        exec_code += "    " + line + "\n"
    
    # Try to compile first to catch syntax errors early
    try:
        compile(exec_code, '<string>', 'exec')
    except SyntaxError as e:
        raise SyntaxError(f"Code compilation failed: {e}\n\nGenerated code:\n{exec_code}")

    # Debug: Save generated code to show what's being executed
    # Write to a temp variable in exec_globals so we can access it if needed
    exec_globals['__generated_code__'] = exec_code
    
    # Execute the function definition
    exec(exec_code, exec_globals)
    
    # Call the function and catch errors to show generated code
    try:
        return await exec_globals["__aexec"](client, message)
    except AttributeError as e:
        # If AttributeError occurs, show the generated code for debugging
        raise AttributeError(f"{e}\n\n=== Generated Code ===\n{exec_code}\n=== End Generated Code ===")


async def edit_or_reply(msg, **kwargs):
    try:
        if msg.from_user.is_self:
            return await msg.edit_text(**kwargs)
        else:
            return await msg.reply(**kwargs)
    except Exception as e:
        print(f"Edit or reply error: {e}")
        return await msg.reply(**kwargs)


@app.on_edited_message(
    filters.command(["evv", "evr"])
    & ~filters.forwarded
    & ~filters.via_bot
)
@app.on_message(
    filters.command(["evv", "evr"])
    & ~filters.forwarded
    & ~filters.via_bot
)
async def executor(client, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    
    # Get raw text from message - Pyrogram's message.text should preserve everything
    # But if Telegram client formats __ as markdown, we need to handle it
    
    # Try to get the original text from message
    # Check if we have entities that might indicate formatting
    original_text = message.text or ""
    
    # If the message has entities and text looks wrong, try to reconstruct
    # For now, just use message.text as-is and hope for the best
    cmd_text = original_text
    
    # Remove the command part (/evv or /evr)
    if cmd_text.startswith('/evv'):
        cmd = cmd_text[4:]
    elif cmd_text.startswith('/evr'):
        cmd = cmd_text[4:]
    else:
        cmd = ""
    
    # Remove only leading newline (preserve spaces/tabs)
    if cmd.startswith('\n'):
        cmd = cmd[1:]
    
    if not cmd or not cmd.strip():
        return await edit_or_reply(message, text="<b>❌ No code was given to execute!</b>")
    
    # DEBUG: Check if __ is in the code
    if '__init__' in cmd or '__' in cmd:
        pass  # Good, double underscores are preserved
    elif 'init(' in cmd and 'def ' in cmd:
        # Try to warn user that __ was stripped
        await message.reply("⚠️ <b>Warning:</b> Double underscores <code>__</code> may have been stripped by Telegram formatting!\n\nTry sending code in a code block or as a document.")
    
    t1 = time()
    old_stderr = sys.stderr
    old_stdout = sys.stdout
    redirected_output = sys.stdout = StringIO()
    redirected_error = sys.stderr = StringIO()
    stdout, stderr, exc = None, None, None
    
    try:
        await aexec(cmd, client, message)
    except Exception:
        exc = traceback.format_exc()
    
    stdout = redirected_output.getvalue()
    stderr = redirected_error.getvalue()
    sys.stdout = old_stdout
    sys.stderr = old_stderr
    
    evaluation = ""
    if exc:
        evaluation = exc
    elif stderr:
        evaluation = stderr
    elif stdout:
        evaluation = stdout
    else:
        evaluation = "✅ SUCCESS - Code executed without output"
    
    final_output = f"<b>📕 EVAL RESULT:</b>\n<pre language='python'>{evaluation}</pre>"
    
    if len(final_output) > 4096:
        filename = "eval_output.txt"
        with open(filename, "w+", encoding="utf8") as out_file:
            out_file.write(str(evaluation))
        t2 = time()
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        text=f"⏳ {round(t2-t1, 3)}s",
                        callback_data=f"runtime {round(t2-t1, 3)} Seconds",
                    )
                ]
            ]
        )
        try:
            await message.reply_document(
                document=filename,
                caption=f"<b>🔗 EVAL COMMAND:</b>\n<code>{cmd[:980]}</code>\n\n<b>📕 RESULT:</b> Attached document",
                quote=False,
                reply_markup=keyboard,
            )
            try:
                await message.delete()
            except:
                pass
        except Exception as e:
            await edit_or_reply(message, text=f"<b>❌ Error sending document:</b>\n<code>{str(e)}</code>")
        finally:
            try:
                os.remove(filename)
            except:
                pass
    else:
        t2 = time()
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        text=f"⏳ {round(t2-t1, 3)}s",
                        callback_data=f"runtime {round(t2-t1, 3)} Seconds",
                    ),
                    InlineKeyboardButton(
                        text="🗑",
                        callback_data=f"forceclose abc|{message.from_user.id}",
                    ),
                ]
            ]
        )
        await edit_or_reply(message, text=final_output, reply_markup=keyboard)


@app.on_callback_query(filters.regex(r"runtime"))
async def runtime_func_cq(_, cq):
    # Silent admin check - no response for non-admins
    if cq.from_user.id not in OWNER_ID:
        return
    try:
        runtime = cq.data.split(None, 1)[1]
        await cq.answer(f"⏱️ Execution Time: {runtime}", show_alert=True)
    except Exception as e:
        await cq.answer(f"❌ Error: {str(e)}", show_alert=True)


@app.on_callback_query(filters.regex("forceclose"))
async def forceclose_command(_, CallbackQuery):
    # Silent admin check - no response for non-admins
    if CallbackQuery.from_user.id not in OWNER_ID:
        return
    
    try:
        callback_data = CallbackQuery.data.strip()
        callback_request = callback_data.split(None, 1)[1]
        query, user_id = callback_request.split("|")
        
        if CallbackQuery.from_user.id != int(user_id):
            return await CallbackQuery.answer(
                "❌ You can only delete your own eval results!", show_alert=True
            )
        
        await CallbackQuery.message.delete()
        await CallbackQuery.answer("🗑️ Message deleted!")
    except Exception as e:
        await CallbackQuery.answer(f"❌ Error: {str(e)}", show_alert=True)




@app.on_edited_message(
    filters.command("shll")
    & ~filters.forwarded
    & ~filters.via_bot
)
@app.on_message(
    filters.command("shll")
    & ~filters.forwarded
    & ~filters.via_bot
)
async def shellrunner(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    
    if len(message.command) < 2:
        return await edit_or_reply(message, text="<b>❌ Usage:</b>\n<code>/shll git pull</code>\n<code>/shll ls -la</code>")
    
    text = message.text.split(None, 1)[1]
    
    try:
        # Execute shell command
        process = subprocess.Popen(
            text,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        try:
            stdout, stderr = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            raise subprocess.TimeoutExpired(process.args, 30)
        
        # Combine output
        output = ""
        if stdout:
            output += stdout
        if stderr:
            output += f"\n--- STDERR ---\n{stderr}"
        
        if not output.strip():
            output = "✅ Command executed successfully (no output)"
        
        # Format the response
        final_output = f"<b>🖥️ SHELL COMMAND:</b>\n<code>{text}</code>\n\n<b>📤 OUTPUT:</b>\n<pre>{output}</pre>"
        
        if len(final_output) > 4096:
            filename = "shell_output.txt"
            with open(filename, "w+", encoding="utf-8") as file:
                file.write(f"Command: {text}\n\nOutput:\n{output}")
            
            try:
                await message.reply_document(
                    document=filename,
                    caption=f"<b>🖥️ SHELL COMMAND:</b>\n<code>{text[:980]}</code>\n\n<b>📤 OUTPUT:</b> Attached document",
                    quote=False
                )
            except Exception as e:
                await edit_or_reply(message, text=f"<b>❌ Error sending document:</b>\n<code>{str(e)}</code>")
            finally:
                try:
                    os.remove(filename)
                except:
                    pass
        else:
            await edit_or_reply(message, text=final_output)
            
    except subprocess.TimeoutExpired:
        await edit_or_reply(message, text="<b>❌ ERROR:</b>\n<code>Command timed out (30s limit)</code>")
    except Exception as err:
        error_msg = f"<b>❌ SHELL ERROR:</b>\n<pre>{str(err)}</pre>"
        await edit_or_reply(message, text=error_msg)


@app.on_message(filters.command("restart"))
async def restart_bot(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        await message.reply("🔄 <b>Restarting bot...</b>\n⏳ Please wait a moment...")
        
        # Give time for the message to be sent
        import asyncio
        await asyncio.sleep(2)
        
        # Method 1: Try using the current working directory and python command
        try:
            if os.name == 'nt':  # Windows
                # Create a batch file to restart the bot
                batch_content = f"""@echo off
cd /d "{os.getcwd()}"
python -m devgagan
pause"""
                with open("restart_bot.bat", "w") as f:
                    f.write(batch_content)
                
                # Start the batch file and exit
                subprocess.Popen(["cmd", "/c", "start", "restart_bot.bat"], 
                               cwd=os.getcwd())
                await asyncio.sleep(1)
                os._exit(0)
            else:  # Linux/Unix
                subprocess.Popen([sys.executable, "-m", "devgagan"])
                os._exit(0)
                
        except Exception as method1_error:
            # Method 2: Try direct python command
            try:
                subprocess.Popen(["python", "-m", "devgagan"], 
                               cwd=os.getcwd())
                os._exit(0)
            except Exception as method2_error:
                # Method 3: Try py command (Windows Python Launcher)
                try:
                    subprocess.Popen(["py", "-m", "devgagan"], 
                                   cwd=os.getcwd())
                    os._exit(0)
                except Exception as method3_error:
                    raise Exception(f"All restart methods failed:\n1: {method1_error}\n2: {method2_error}\n3: {method3_error}")
        
    except Exception as e:
        await message.reply(f"<b>❌ Restart failed:</b>\n<code>{str(e)}</code>")


@app.on_message(filters.command("kill"))
async def kill_bot(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        await message.reply("💀 <b>Stopping bot...</b>\n⚠️ You'll need to manually restart it!")
        
        # Give time for the message to be sent
        import asyncio
        await asyncio.sleep(1)
        
        # Just exit the process
        os._exit(0)
        
    except Exception as e:
        await message.reply(f"<b>❌ Kill failed:</b>\n<code>{str(e)}</code>")
