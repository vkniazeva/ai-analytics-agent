import { useState } from 'react'
import type { FormEvent } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Button } from '../components/Button'
import { Card } from '../components/Card'
import { Spinner } from '../components/Spinner'
import { useChatStore } from '../lib/chatStore'

export function ChatView() {
  const { messages, loading, error, sendMessage } = useChatStore()
  const [question, setQuestion] = useState('')

  function handleSubmit(e: FormEvent) {
    e.preventDefault()
    const trimmed = question.trim()
    if (!trimmed || loading) return
    setQuestion('')
    void sendMessage(trimmed)
  }

  return (
    <div className="mx-auto flex h-full max-w-2xl flex-col gap-4">
      <h1 className="text-xl font-semibold text-gray-900">Chat</h1>

      <Card className="flex flex-1 flex-col gap-3 overflow-y-auto">
        {messages.length === 0 && (
          <p className="text-sm text-gray-500">Ask a question to begin.</p>
        )}
        {messages.map((message) => (
          <div
            key={message.id}
            className={`max-w-[80%] rounded-lg px-3 py-2 text-sm ${
              message.role === 'user'
                ? 'ml-auto bg-blue-600 text-white'
                : 'bg-gray-100 text-gray-800'
            }`}
          >
            {message.role === 'assistant' ? (
              <div className="prose prose-sm max-w-none prose-p:my-1 prose-ul:my-1 prose-li:my-0">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                  {message.content}
                </ReactMarkdown>
              </div>
            ) : (
              message.content
            )}
          </div>
        ))}
        {loading && (
          <div className="flex items-center gap-2">
            <Spinner />
            <span className="text-sm text-gray-500">Thinking...</span>
          </div>
        )}
      </Card>

      {error && <p className="text-sm text-red-600">{error}</p>}

      <form onSubmit={handleSubmit} className="flex gap-2">
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="Ask a question about your data..."
          className="flex-1 rounded-md border border-gray-300 px-3 py-2 text-sm"
          disabled={loading}
        />
        <Button type="submit" disabled={loading || !question.trim()}>
          Send
        </Button>
      </form>
    </div>
  )
}
