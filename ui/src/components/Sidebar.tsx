import { MessageSquare, LineChart, Plus, Settings } from 'lucide-react'
import { NavLink, useLocation } from 'react-router-dom'
import { useChatStore } from '../lib/chatStore'
import { MODULES, moduleForPath } from '../lib/modules'
import logo from '../assets/logo-dark-bg.svg'

const NAV_ICONS = {
  chat: MessageSquare,
  forecast: LineChart,
}

export function Sidebar() {
  const location = useLocation()
  const activeModule = moduleForPath(location.pathname).key
  const { messages, startNewSession } = useChatStore()

  const hasSession = messages.length > 0
  const lastMessage = messages[messages.length - 1]

  return (
    <nav
      className="flex w-[268px] shrink-0 flex-col bg-[var(--color-800)] px-[18px] pb-[18px] pt-[22px] text-white"
      style={{ minHeight: 820 }}
    >
      <img src={logo} alt="Omnevo" className="h-[26px] w-auto pb-[22px]" />

      <div
        className="flex flex-col gap-1 pb-5"
        style={{ borderBottom: '1px solid rgba(255,255,255,.09)' }}
      >
        {MODULES.map((mod) => {
          const Icon = NAV_ICONS[mod.key]
          const active = mod.key === activeModule
          return (
            <NavLink
              key={mod.key}
              to={mod.path}
              className={`flex items-center gap-[11px] rounded-[10px] px-3 py-2.5 text-sm transition-colors duration-150 ease-[var(--ease-standard)] ${
                active
                  ? 'bg-purple-main font-bold text-white'
                  : 'font-medium text-white/68 hover:text-white/90'
              }`}
            >
              <Icon size={20} />
              {mod.navLabel}
            </NavLink>
          )
        })}
      </div>

      <div className="flex flex-1 flex-col overflow-y-auto pt-5">
        <div
          className={`mb-2 flex items-center justify-between transition-opacity duration-150 ${
            activeModule === 'chat' ? 'opacity-100' : 'opacity-0'
          }`}
        >
          <span className="text-[11px] font-bold uppercase tracking-[0.09em] text-white/42">
            Recent sessions
          </span>
          <button
            type="button"
            aria-label="New session"
            onClick={startNewSession}
            className="text-white/68 hover:text-white"
          >
            <Plus size={18} />
          </button>
        </div>

        {activeModule === 'chat' && hasSession && (
          <button
            type="button"
            className="rounded-[10px] bg-white/9 px-3 py-[9px] text-left"
          >
            <div className="truncate text-[13.5px] font-semibold text-white/92">
              {messages[0]?.content ?? 'New session'}
            </div>
            <div className="mt-[3px] text-[11.5px] text-white/40">
              {lastMessage?.role === 'user' ? 'Awaiting reply' : 'Replied'}
            </div>
          </button>
        )}
      </div>

      <div
        className="flex items-center gap-3 px-2 pb-1 pt-3.5"
        style={{ borderTop: '1px solid rgba(255,255,255,.09)' }}
      >
        <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-purple-bright text-[12.5px] font-bold">
          VK
        </div>
        <div className="flex-1 overflow-hidden">
          <div className="truncate text-[13px] font-semibold">
            Viktoriia Kniazeva
          </div>
          <div className="truncate text-[11.5px] text-white/45">
            Commercial analytics
          </div>
        </div>
        <Settings size={20} className="text-white/68" />
      </div>
    </nav>
  )
}
