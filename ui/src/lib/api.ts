import type { AskRequest, AskResponse } from '../types/chat'
import type {
  PredictItem,
  PredictItemDetail,
  PredictRequest,
  ThresholdType,
} from '../types/forecasting'

const ANALYTICS_API_BASE = 'http://localhost:8001'
const FORECASTING_API_BASE = 'http://localhost:8000'

async function parseJsonOrThrow<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const body = await response.text()
    throw new Error(`${response.status} ${response.statusText}: ${body}`)
  }
  return response.json() as Promise<T>
}

export async function postAsk(request: AskRequest): Promise<AskResponse> {
  const response = await fetch(`${ANALYTICS_API_BASE}/ask`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  })
  return parseJsonOrThrow(response)
}

export async function postPredict(
  thresholdType: ThresholdType,
  request: PredictRequest,
): Promise<PredictItem[]> {
  const response = await fetch(
    `${FORECASTING_API_BASE}/predict/${thresholdType}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    },
  )
  return parseJsonOrThrow(response)
}

export async function postPredictItemDetail(
  thresholdType: ThresholdType,
  itemId: string,
  request: PredictRequest,
): Promise<PredictItemDetail> {
  const response = await fetch(
    `${FORECASTING_API_BASE}/predict/${thresholdType}/item/${encodeURIComponent(itemId)}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    },
  )
  return parseJsonOrThrow(response)
}
