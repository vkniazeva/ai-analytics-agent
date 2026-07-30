export type Domain =
  | 'sales'
  | 'wastage'
  | 'flights'
  | 'product_catalog'
  | 'pax_sales'

export type ChartType = 'line' | 'bar'

export interface ChartConfig {
  id: string
  title: string
  domain: Domain
  metrics: string[]
  groupBy: string[]
  chartType: ChartType
}

export interface DashboardMetadataResponse {
  metrics: Record<string, string[]>
  dimensions: Record<string, string[]>
}

export interface DashboardMetricsResponse {
  rows: Record<string, string | number>[]
  truncated: boolean
}
