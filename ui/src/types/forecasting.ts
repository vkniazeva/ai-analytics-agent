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
  predicted_value: number
  threshold: number
  hist_avg: number
  hist_level_used: number | null
  hist_level_description: string | null
  missed_sale_probability: number | null
  wastage_probability: number | null
  sample_size: number | null
  metrics_level_used: number | null
  metrics_level_description: string | null
}
