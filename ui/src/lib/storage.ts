import type { ChartConfig } from '../types/dashboard'

const CHARTS_KEY = 'ai-analytics.dashboard-charts'
const CONVERSATION_ID_KEY = 'ai-analytics.conversation-id'

export function loadCharts(): ChartConfig[] {
  const raw = localStorage.getItem(CHARTS_KEY)
  if (!raw) return []
  try {
    return JSON.parse(raw) as ChartConfig[]
  } catch {
    return []
  }
}

export function saveCharts(charts: ChartConfig[]): void {
  localStorage.setItem(CHARTS_KEY, JSON.stringify(charts))
}

export function loadConversationId(): string | null {
  return localStorage.getItem(CONVERSATION_ID_KEY)
}

export function saveConversationId(conversationId: string): void {
  localStorage.setItem(CONVERSATION_ID_KEY, conversationId)
}

export function clearConversationId(): void {
  localStorage.removeItem(CONVERSATION_ID_KEY)
}
