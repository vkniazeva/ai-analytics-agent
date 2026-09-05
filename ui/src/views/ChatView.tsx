import { ArrowUp, Database, Sparkles } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import type { KeyboardEvent } from 'react'
import { MessageBlocks } from '../components/chat/MessageBlocks'
import { useChatStore } from '../lib/chatStore'
import { blocksFromMessage } from '../types/chatBlocks'

const MAX_TEXTAREA_HEIGHT = 120

const DATA_DOMAINS: Array<{ title: string; body: string }> = [
  {
    title: 'Sales',
    body: 'revenue, quantity, discounts, average sale — by year, month, route, category or item',
  },
  {
    title: 'Wastage',
    body: 'loaded / sold / wasted quantities, fresh vs non-fresh — by category, item, destination',
  },
  {
    title: 'Flights & passengers',
    body: 'flight counts, passenger counts, avg sale per passenger — by date, route, day period',
  },
  {
    title: 'Product catalog',
    body: 'item counts by category and item type',
  },
]

const EXAMPLE_QUESTIONS: string[] = [
  '10 best selling items by revenue in 2025',
  'Wastage by category for December 2025, sorted from worst',
  'Compare revenue in December 2025 vs January 2026 by route',
  'Average sale per passenger by route, top 5',
  'Show revenue by month in 2025 as a chart',
]

function TypingIndicator() {
  return (
    <div className="flex items-center gap-[5px] py-1">
      {[0, 0.16, 0.32].map((delay) => (
        <span
          key={delay}
          className="h-[7px] w-[7px] rounded-full bg-purple-main"
          style={{ animation: `omBlink 1s ${delay}s infinite` }}
        />
      ))}
    </div>
  )
}

export function ChatView() {
  const { messages, loading, error, sendMessage } = useChatStore()
  const [draft, setDraft] = useState('')
  const threadRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  useEffect(() => {
    const el = threadRef.current
    if (!el) return
    el.scrollTop = el.scrollHeight
  }, [messages, loading])

  useEffect(() => {
    const el = textareaRef.current
    if (!el) return
    el.style.height = 'auto'
    el.style.height = `${Math.min(el.scrollHeight, MAX_TEXTAREA_HEIGHT)}px`
  }, [draft])

  function submit(text: string) {
    const trimmed = text.trim()
    if (!trimmed || loading) return
    setDraft('')
    void sendMessage(trimmed)
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      submit(draft)
    }
  }

  return (
    <div className="flex h-full flex-col">
      <div ref={threadRef} className="flex-1 overflow-y-auto px-8 pb-2 pt-8">
        <div className="mx-auto flex max-w-[920px] flex-col gap-[26px]">
          {messages.length === 0 && (
            <div className="flex flex-col gap-6">
              <div className="flex gap-3.5">
                <div className="flex h-[34px] w-[34px] shrink-0 items-center justify-center rounded-[10px] bg-[var(--purple-light)]">
                  <Sparkles size={20} color="var(--purple-deep)" />
                </div>
                <div className="flex flex-col gap-2">
                  <p className="text-[15px] font-medium text-[var(--text-strong)]">
                    Ask about the onboard sales warehouse.
                  </p>
                  <p className="text-[13.5px] leading-[1.55] text-[var(--text-muted)]">
                    I can answer questions across sales, wastage, flights, passengers and the product catalog. Group results by any dimension, filter by period or route, or request a chart.
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-2.5 sm:grid-cols-2">
                {DATA_DOMAINS.map((domain) => (
                  <div
                    key={domain.title}
                    className="rounded-[12px] border border-[var(--border-subtle)] bg-white px-4 py-3"
                  >
                    <p className="text-[13px] font-semibold text-[var(--text-strong)]">
                      {domain.title}
                    </p>
                    <p className="mt-1 text-[12.5px] leading-[1.5] text-[var(--text-muted)]">
                      {domain.body}
                    </p>
                  </div>
                ))}
              </div>

              <div className="flex flex-col gap-2">
                <p className="text-[12px] font-medium uppercase tracking-wide text-[var(--text-faint)]">
                  Try one of these
                </p>
                <div className="flex flex-wrap gap-2">
                  {EXAMPLE_QUESTIONS.map((question) => (
                    <button
                      key={question}
                      type="button"
                      onClick={() => {
                        setDraft(question)
                        textareaRef.current?.focus()
                      }}
                      className="rounded-full border border-[var(--border-subtle)] bg-white px-3.5 py-1.5 text-[12.5px] text-[var(--text-strong)] transition-colors hover:border-purple-main hover:text-purple-main"
                    >
                      {question}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          )}
          {messages.map((message) => (
            <div
              key={message.id}
              style={{ animation: 'omRise 0.3s ease' }}
              className={message.role === 'user' ? 'flex justify-end' : 'flex gap-3.5'}
            >
              {message.role === 'assistant' && (
                <div className="flex h-[34px] w-[34px] shrink-0 items-center justify-center rounded-[10px] bg-[var(--purple-light)]">
                  <Sparkles size={20} color="var(--purple-deep)" />
                </div>
              )}
              {message.role === 'user' ? (
                <div className="max-w-[560px] rounded-[16px_16px_4px_16px] bg-purple-main px-[18px] py-3.5 text-[14.5px] leading-[1.55] text-white">
                  {message.content}
                </div>
              ) : (
                <MessageBlocks blocks={blocksFromMessage(message)} />
              )}
            </div>
          ))}
          {loading && (
            <div className="flex gap-3.5">
              <div className="flex h-[34px] w-[34px] shrink-0 items-center justify-center rounded-[10px] bg-[var(--purple-light)]">
                <Sparkles size={20} color="var(--purple-deep)" />
              </div>
              <TypingIndicator />
            </div>
          )}
          {error && <p className="text-sm text-[var(--danger-500)]">{error}</p>}
        </div>
      </div>

      <div
        className="px-8 pb-6 pt-3"
        style={{
          background:
            'linear-gradient(to top, var(--color-200) 70%, rgba(247,246,248,0))',
        }}
      >
        <div className="mx-auto flex max-w-[920px] flex-col gap-2.5">
          <div className="flex items-end gap-2.5 rounded-[var(--radius-lg)] border-[1.5px] border-[var(--border-subtle)] bg-white p-2.5 pl-4 shadow-[var(--shadow-sm)]">
            <Database size={21} className="mb-2 shrink-0 text-[var(--text-faint)]" />
            <textarea
              ref={textareaRef}
              rows={1}
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask about sales, routes, categories - or ask for a chart"
              className="max-h-[120px] flex-1 resize-none bg-transparent text-[14.5px] leading-[1.5] text-[var(--text-strong)] outline-none placeholder:text-[var(--text-faint)]"
            />
            <button
              type="button"
              onClick={() => submit(draft)}
              disabled={loading || !draft.trim()}
              aria-label="Send"
              className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[12px] bg-purple-main text-white transition-colors hover:bg-purple-deep disabled:cursor-not-allowed disabled:opacity-50"
            >
              <ArrowUp size={21} />
            </button>
          </div>

          <p className="text-center text-[11.5px] text-[var(--text-muted)]">
            Answers are generated from the onboard sales warehouse - 14.2M
            transactions, refreshed hourly.
          </p>
        </div>
      </div>
    </div>
  )
}
