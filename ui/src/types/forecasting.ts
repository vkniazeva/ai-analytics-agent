export type ThresholdType = 'low_missed_sales' | 'low_wastage'

export type DayPeriod = 'Morning' | 'Day' | 'Evening' | 'Night'

export interface PredictRequest {
  route: string
  expected_pax: number
  day_period: DayPeriod
}

export interface PredictItem {
  item_id: string
  predicted_quantity: number
}

export interface PredictItemDetail {
  item_id: string
  threshold_type: ThresholdType
  threshold_value: number
  predicted_quantity: number
  historical_average: number
  estimated_accuracy: number | null
}
