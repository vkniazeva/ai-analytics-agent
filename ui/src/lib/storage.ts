const CONVERSATION_ID_KEY = 'ai-analytics.conversation-id'

export function loadConversationId(): string | null {
  return localStorage.getItem(CONVERSATION_ID_KEY)
}

export function saveConversationId(conversationId: string): void {
  localStorage.setItem(CONVERSATION_ID_KEY, conversationId)
}

export function clearConversationId(): void {
  localStorage.removeItem(CONVERSATION_ID_KEY)
}
