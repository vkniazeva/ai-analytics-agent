import type { DayPeriod, PredictRequest, ThresholdType } from '../types/forecasting'

export type DepartWindow = 'morning' | 'day' | 'evening' | 'night'
export type Objective = 'low_wastage' | 'no_lost_sales'

export const DEPART_WINDOWS: { value: DepartWindow; label: string }[] = [
  { value: 'morning', label: 'Morning' },
  { value: 'day', label: 'Day' },
  { value: 'evening', label: 'Evening' },
  { value: 'night', label: 'Night' },
]

export interface ObjectiveDef {
  key: Objective
  label: string
  threshold: number
  tint: string
  border: string
  ink: string
  copy: string
}

export const OBJECTIVES: ObjectiveDef[] = [
  {
    key: 'low_wastage',
    label: 'Minimise wastage',
    threshold: 0.7,
    tint: 'var(--success-100)',
    border: '#bfeeda',
    ink: 'var(--green-deep)',
    copy:
      'The classification model flags an item as a likely sale only when its predicted probability of selling is above 70%. Fewer items clear that bar, so less stock is loaded and wastage drops, at the cost of some slower movers being under-stocked.',
  },
  {
    key: 'no_lost_sales',
    label: 'Avoid lost sales',
    threshold: 0.4,
    tint: 'var(--warning-100)',
    border: '#ffdba6',
    ink: '#a35f00',
    copy:
      'The classification model flags an item as a likely sale once its predicted probability of selling is above 40%. More items clear that lower bar, so more stock is loaded and lost sales drop, at the cost of carrying items that are less likely to sell.',
  },
]

const DAY_PERIOD_MAP: Record<DepartWindow, DayPeriod> = {
  morning: 'Morning',
  day: 'Day',
  evening: 'Evening',
  night: 'Night',
}

const OBJECTIVE_TO_API: Record<Objective, ThresholdType> = {
  low_wastage: 'low_wastage',
  no_lost_sales: 'low_missed_sales',
}

const API_TO_OBJECTIVE: Record<ThresholdType, Objective> = {
  low_wastage: 'low_wastage',
  low_missed_sales: 'no_lost_sales',
}

export function toPredictRequest(params: {
  origin: string
  destination: string
  pax: number
  depart: DepartWindow
}): PredictRequest {
  return {
    route: `${params.origin} _ ${params.destination}`,
    expected_pax: params.pax,
    day_period: DAY_PERIOD_MAP[params.depart],
  }
}

export function toApiThresholdType(objective: Objective): ThresholdType {
  return OBJECTIVE_TO_API[objective]
}

export function toDisplayObjective(thresholdType: ThresholdType): Objective {
  return API_TO_OBJECTIVE[thresholdType]
}

export function objectiveDef(objective: Objective): ObjectiveDef {
  return OBJECTIVES.find((o) => o.key === objective) ?? OBJECTIVES[0]
}
