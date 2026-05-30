import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

SCRIPTS_DIR = Path(__file__).parent / "scripts"

ALLOWLIST = {
    "all-leads.py",
    "enquires.py",
    "requirement_enquiries.py",
    "leads.py",
    "inventories-from-firebase.py",
    "new-inventory.py",
    "new-inventory-2.py",
    "QC.py",
    "req.py",
    "agents.py",
    "connecthistory.py",
    "connecthistory_leads.py",
    "truestate-sync.py",
}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/run")
async def run_script(
    script: str = Query(...),
    db: str = Query("new"),
):
    if script not in ALLOWLIST:
        return Response("Forbidden", status_code=403)

    script_path = (SCRIPTS_DIR / script).resolve()
    if not str(script_path).startswith(str(SCRIPTS_DIR.resolve())):
        return Response("Forbidden", status_code=403)

    db_arg = "old" if db == "old" else "new"

    async def generate():
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script_path),
            "--db",
            db_arg,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(SCRIPTS_DIR),
            env={**os.environ},
        )

        q: asyncio.Queue = asyncio.Queue()

        async def pipe(stream, prefix: str = ""):
            while True:
                line = await stream.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    await q.put(
                        f"event: log\ndata: {json.dumps(prefix + text)}\n\n"
                    )
            await q.put(None)

        stdout_task = asyncio.create_task(pipe(proc.stdout))
        stderr_task = asyncio.create_task(pipe(proc.stderr, "[err] "))

        done = 0
        while done < 2:
            item = await q.get()
            if item is None:
                done += 1
            else:
                yield item

        await asyncio.gather(stdout_task, stderr_task)
        await proc.wait()

        if proc.returncode == 0:
            yield 'event: done\ndata: "0"\n\n'
        else:
            yield 'event: error-event\ndata: "1"\n\n'

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
