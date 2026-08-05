export interface ChartSpec {
  chart_type: 'bar' | 'line'
  x_key: string
  series_keys: string[]
  data: Record<string, string | number>[]
  title: string
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  chart?: ChartSpec
}

export interface AskRequest {
  question: string
  conversation_id?: string
}

export interface AskResponse {
  answer: string
  conversation_id: string
  chart?: ChartSpec | null
}
