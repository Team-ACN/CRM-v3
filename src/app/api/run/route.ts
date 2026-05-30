import { spawn } from 'child_process'
import path from 'path'
import { SCRIPT_ALLOWLIST } from '@/lib/scripts'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url)
  const script = searchParams.get('script') ?? ''
  const db = searchParams.get('db') === 'old' ? 'old' : 'new'

  if (!SCRIPT_ALLOWLIST.has(script)) {
    return new Response('Forbidden', { status: 403 })
  }

  const scriptsDir = path.join(process.cwd(), 'scripts')
  const scriptPath = path.join(scriptsDir, script)

  if (!scriptPath.startsWith(scriptsDir + path.sep)) {
    return new Response('Forbidden', { status: 403 })
  }

  const encoder = new TextEncoder()

  const stream = new ReadableStream({
    start(controller) {
      const send = (event: string, data: string) => {
        try {
          controller.enqueue(
            encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
          )
        } catch {
          // controller already closed
        }
      }

      const pythonBin = process.env.PYTHON_BIN ?? 'python3'

      const proc = spawn(pythonBin, [scriptPath, '--db', db], {
        env: { ...process.env },
        cwd: scriptsDir,
        stdio: ['ignore', 'pipe', 'pipe'],
      })

      let buf = ''

      const flush = (chunk: string, prefix = '') => {
        buf += chunk
        const parts = buf.split('\n')
        buf = parts.pop() ?? ''
        for (const part of parts) {
          if (part.trim()) send('log', prefix + part)
        }
      }

      proc.stdout.setEncoding('utf8')
      proc.stderr.setEncoding('utf8')
      proc.stdout.on('data', (d: string) => flush(d))
      proc.stderr.on('data', (d: string) => flush(d, '[err] '))

      proc.on('close', (code) => {
        if (buf.trim()) send('log', buf)
        send(code === 0 ? 'done' : 'error-event', String(code ?? 1))
        controller.close()
      })

      proc.on('error', (err) => {
        send('log', `[spawn error] ${err.message}`)
        send('error-event', '1')
        controller.close()
      })
    },
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  })
}
