export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
}

export interface AskRequest {
  question: string
  conversation_id?: string
}

export interface AskResponse {
  answer: string
  conversation_id: string
}
