import { NavLink } from 'react-router-dom'

const NAV_ITEMS = [
  { to: '/dashboards', label: 'Dashboards' },
  { to: '/chat', label: 'Chat' },
  { to: '/forecasting', label: 'Forecasting' },
]

export function Sidebar() {
  return (
    <nav className="flex w-56 shrink-0 flex-col gap-1 border-r border-gray-200 bg-white p-4">
      <span className="mb-4 px-2 text-lg font-semibold text-gray-900">
        AI Analytics
      </span>
      {NAV_ITEMS.map((item) => (
        <NavLink
          key={item.to}
          to={item.to}
          className={({ isActive }) =>
            `rounded-md px-3 py-2 text-sm font-medium ${
              isActive
                ? 'bg-blue-50 text-blue-700'
                : 'text-gray-600 hover:bg-gray-50'
            }`
          }
        >
          {item.label}
        </NavLink>
      ))}
    </nav>
  )
}
