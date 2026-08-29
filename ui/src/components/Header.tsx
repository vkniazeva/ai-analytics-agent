import { MessageSquare, LineChart } from 'lucide-react'
import { useLocation, useNavigate } from 'react-router-dom'
import { Tabs } from './Tabs'
import { MODULES, moduleForPath } from '../lib/modules'

const TAB_ICONS = {
  chat: MessageSquare,
  forecast: LineChart,
}

export function Header() {
  const location = useLocation()
  const navigate = useNavigate()
  const activeModule = moduleForPath(location.pathname)

  return (
    <header className="border-b border-[var(--border-subtle)] bg-white px-8 pt-[18px]">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-[21px] font-bold tracking-[-0.02em] text-[var(--text-strong)]">
            {activeModule.headerTitle}
          </h1>
          <p className="mt-[5px] max-w-[640px] text-[13.5px] text-[var(--text-muted)]" style={{ textWrap: 'pretty' }}>
            {activeModule.subtitle}
          </p>
        </div>
      </div>

      <div className="mt-4">
        <Tabs
          value={activeModule.key}
          onChange={(value) => {
            const target = MODULES.find((m) => m.key === value)
            if (target) navigate(target.path)
          }}
          items={MODULES.map((m) => ({
            value: m.key,
            label: m.navLabel,
            icon: (() => {
              const Icon = TAB_ICONS[m.key]
              return <Icon size={18} />
            })(),
          }))}
        />
      </div>
    </header>
  )
}
