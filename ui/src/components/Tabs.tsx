import type { ReactNode } from 'react'

interface TabItem {
  value: string
  label: string
  icon?: ReactNode
}

interface TabsProps {
  items: TabItem[]
  value: string
  onChange: (value: string) => void
}

export function Tabs({ items, value, onChange }: TabsProps) {
  return (
    <div
      role="tablist"
      className="flex gap-1 border-b border-[var(--border-subtle)]"
    >
      {items.map((item) => {
        const active = item.value === value
        return (
          <button
            key={item.value}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => onChange(item.value)}
            className={`flex items-center gap-2 border-b-2 px-1 pb-3 text-sm font-medium transition-colors duration-150 ease-[var(--ease-standard)] ${
              active
                ? 'border-purple-main text-purple-main font-bold'
                : 'border-transparent text-[var(--text-muted)] hover:text-[var(--text-body)]'
            }`}
          >
            {item.icon}
            {item.label}
          </button>
        )
      })}
    </div>
  )
}
