export interface ChartSeries {
  label: string
  color: string
}

export interface ChartBlock {
  type: 'chart'
  title: string
  caption?: string
  series: ChartSeries[]
  categories: string[]
  /** values[categoryIndex][seriesIndex], each 0-1 as a share of the tallest bar */
  values: number[][]
}

export interface TableBlock {
  type: 'table'
  columns: string[]
  rows: (string | number)[][]
}

export interface InsightBlock {
  type: 'insight'
  text: string
}

export interface ProseBlock {
  type: 'prose'
  text: string
}

export type MessageBlock = ProseBlock | ChartBlock | TableBlock | InsightBlock

/**
 * The real /ask endpoint only returns markdown prose today. This wraps that
 * text as a single prose block so the renderer's block union is exercised
 * end to end; chart/table/insight blocks activate once the API returns them.
 */
export function blocksFromAnswer(answer: string): MessageBlock[] {
  return [{ type: 'prose', text: answer }]
}
