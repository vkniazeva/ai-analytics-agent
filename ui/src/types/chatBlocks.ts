import type { ChatMessage } from './chat'

export interface ChartBlock {
  type: 'chart'
  chartType: 'bar' | 'line'
  xKey: string
  seriesKeys: string[]
  data: Record<string, string | number>[]
  title: string
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

export function blocksFromMessage(message: ChatMessage): MessageBlock[] {
  const blocks: MessageBlock[] = [{ type: 'prose', text: message.content }]

  if (message.chart) {
    blocks.push({
      type: 'chart',
      chartType: message.chart.chart_type,
      xKey: message.chart.x_key,
      seriesKeys: message.chart.series_keys,
      data: message.chart.data,
      title: message.chart.title,
    })
  }

  return blocks
}
