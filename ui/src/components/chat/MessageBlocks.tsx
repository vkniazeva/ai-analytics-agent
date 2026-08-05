import { Lightbulb } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import type {
  ChartBlock,
  InsightBlock,
  MessageBlock,
  ProseBlock,
  TableBlock,
} from '../../types/chatBlocks'
import { AnalyticsChart, SERIES_COLORS } from '../AnalyticsChart'

function ProseBlockView({ block }: { block: ProseBlock }) {
  return (
    <div className="prose prose-sm max-w-none text-[14.5px] leading-[1.65] text-[var(--text-body)] prose-p:my-0 prose-p:text-[var(--text-body)] prose-strong:font-bold prose-strong:text-[var(--text-strong)] prose-ul:my-1 prose-li:my-0">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{block.text}</ReactMarkdown>
    </div>
  )
}

function ChartBlockView({ block }: { block: ChartBlock }) {
  return (
    <div className="overflow-hidden rounded-[var(--radius-md)] border border-[var(--border-subtle)] bg-white shadow-[var(--shadow-sm)]">
      <div className="flex items-start justify-between gap-3 px-5 pt-4">
        <div className="text-sm font-bold text-[var(--text-strong)]">
          {block.title}
        </div>
        <div className="flex flex-wrap justify-end gap-3">
          {block.seriesKeys.map((key, index) => (
            <span
              key={key}
              className="flex items-center gap-1.5 text-[11.5px] text-[var(--text-body)]"
            >
              <span
                className="h-[9px] w-[9px] rounded-[3px]"
                style={{ backgroundColor: SERIES_COLORS[index % SERIES_COLORS.length] }}
              />
              {key}
            </span>
          ))}
        </div>
      </div>

      <div className="px-3 pb-4 pt-3">
        <AnalyticsChart
          chartType={block.chartType}
          xKey={block.xKey}
          seriesKeys={block.seriesKeys}
          data={block.data}
          height={360}
          showLegend={false}
        />
      </div>
    </div>
  )
}

function TableBlockView({ block }: { block: TableBlock }) {
  const gridTemplateColumns = `1.6fr repeat(${Math.max(0, block.columns.length - 1)}, 1fr)`
  return (
    <div className="overflow-hidden rounded-[var(--radius-md)] border border-[var(--border-subtle)] bg-white">
      <div
        className="grid gap-3 bg-[var(--color-200)] px-5 py-3 text-[11.5px] font-bold uppercase tracking-[0.06em] text-[var(--text-muted)]"
        style={{ gridTemplateColumns }}
      >
        {block.columns.map((col, i) => (
          <div key={col} className={i > 0 ? 'text-right' : ''}>
            {col}
          </div>
        ))}
      </div>
      {block.rows.map((row, rowIndex) => (
        <div
          key={rowIndex}
          className="grid gap-3 border-t border-[var(--border-subtle)] px-5 py-[13px] text-[13.5px]"
          style={{ gridTemplateColumns }}
        >
          {row.map((cell, i) => (
            <div
              key={i}
              className={
                i === 0
                  ? 'font-semibold text-[var(--text-strong)]'
                  : 'text-right tabular-nums text-[var(--text-body)]'
              }
            >
              {cell}
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}

function InsightBlockView({ block }: { block: InsightBlock }) {
  return (
    <div
      className="flex items-start gap-2.5 rounded-xl p-4"
      style={{
        backgroundColor: 'var(--info-100)',
        border: '1px solid var(--info-border)',
      }}
    >
      <Lightbulb size={20} className="mt-0.5 shrink-0" style={{ color: 'var(--purple-deep)' }} />
      <p className="text-[13.5px] leading-[1.6]" style={{ color: 'var(--purple-deep)' }}>
        {block.text}
      </p>
    </div>
  )
}

export function MessageBlocks({ blocks }: { blocks: MessageBlock[] }) {
  return (
    <div className="flex flex-col gap-4">
      {blocks.map((block, i) => {
        switch (block.type) {
          case 'prose':
            return <ProseBlockView key={i} block={block} />
          case 'chart':
            return <ChartBlockView key={i} block={block} />
          case 'table':
            return <TableBlockView key={i} block={block} />
          case 'insight':
            return <InsightBlockView key={i} block={block} />
          default:
            return null
        }
      })}
    </div>
  )
}
