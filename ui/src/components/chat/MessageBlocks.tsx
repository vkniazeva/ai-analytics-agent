import { Code2, Download, Lightbulb, Table as TableIcon } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import type {
  ChartBlock,
  InsightBlock,
  MessageBlock,
  ProseBlock,
  TableBlock,
} from '../../types/chatBlocks'

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
        <div>
          <div className="text-sm font-bold text-[var(--text-strong)]">
            {block.title}
          </div>
          {block.caption && (
            <div className="mt-0.5 text-xs text-[var(--text-muted)]">
              {block.caption}
            </div>
          )}
        </div>
        <div className="flex gap-3">
          {block.series.map((s) => (
            <span
              key={s.label}
              className="flex items-center gap-1.5 text-[11.5px] text-[var(--text-body)]"
            >
              <span
                className="h-[9px] w-[9px] rounded-[3px]"
                style={{ backgroundColor: s.color }}
              />
              {s.label}
            </span>
          ))}
        </div>
      </div>

      <div
        className="grid items-end gap-[18px] px-[22px] pt-3"
        style={{
          gridTemplateColumns: `repeat(${block.categories.length}, 1fr)`,
          height: 208,
        }}
      >
        {block.values.map((columnValues, i) => (
          <div key={i} className="flex h-full items-end gap-1">
            {columnValues.map((value, seriesIndex) => (
              <div
                key={seriesIndex}
                className="flex-1 rounded-t-[5px]"
                style={{
                  height: `${Math.max(0, Math.min(1, value)) * 100}%`,
                  backgroundColor: block.series[seriesIndex]?.color,
                }}
              />
            ))}
          </div>
        ))}
      </div>
      <div
        className="grid gap-[18px] px-[22px] pb-3 pt-2 text-center text-[11.5px] text-[var(--text-muted)]"
        style={{ gridTemplateColumns: `repeat(${block.categories.length}, 1fr)` }}
      >
        {block.categories.map((c) => (
          <div key={c}>{c}</div>
        ))}
      </div>

      <div className="flex items-center justify-between border-t border-[var(--border-subtle)] bg-[var(--color-200)] px-5 py-2.5">
        <div className="flex gap-1">
          <button
            type="button"
            className="flex items-center gap-1.5 rounded-[8px] px-2.5 py-1.5 text-[12.5px] font-semibold text-[var(--text-body)] hover:bg-[var(--color-300)]"
          >
            <Download size={14} /> PNG
          </button>
          <button
            type="button"
            className="flex items-center gap-1.5 rounded-[8px] px-2.5 py-1.5 text-[12.5px] font-semibold text-[var(--text-body)] hover:bg-[var(--color-300)]"
          >
            <TableIcon size={14} /> CSV
          </button>
        </div>
        <button
          type="button"
          className="flex items-center gap-1.5 rounded-[8px] px-2.5 py-1.5 text-[12.5px] font-semibold text-[var(--text-body)] hover:bg-[var(--color-300)]"
        >
          <Code2 size={14} /> Show query
        </button>
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
