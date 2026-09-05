import { create } from 'zustand'
import type { ChatMessage } from '../types/chat'
import { postAsk } from './api'
import {
  clearConversationId,
  loadConversationId,
  saveConversationId,
} from './storage'

function generateId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`
}

interface ChatState {
  messages: ChatMessage[]
  conversationId: string | null
  loading: boolean
  error: string | null
  sendMessage: (question: string) => Promise<void>
  startNewSession: () => void
}

export const useChatStore = create<ChatState>((set, get) => ({
  messages: [],
  conversationId: loadConversationId(),
  loading: false,
  error: null,

  sendMessage: async (question) => {
    const userMessage: ChatMessage = {
      id: generateId(),
      role: 'user',
      content: question,
    }
    set((state) => ({
      messages: [...state.messages, userMessage],
      loading: true,
      error: null,
    }))

    try {
      const response = await postAsk({
        question,
        conversation_id: get().conversationId ?? undefined,
      })
      saveConversationId(response.conversation_id)
      const assistantMessage: ChatMessage = {
        id: generateId(),
        role: 'assistant',
        content: response.answer,
        chart: response.chart ?? undefined,
      }
      set((state) => ({
        messages: [...state.messages, assistantMessage],
        conversationId: response.conversation_id,
        loading: false,
      }))
    } catch (err) {
      set({
        loading: false,
        error: err instanceof Error ? err.message : 'Failed to get a response',
      })
    }
  },

  startNewSession: () => {
    clearConversationId()
    set({ messages: [], conversationId: null, error: null })
  },
}))
