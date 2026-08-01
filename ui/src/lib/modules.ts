export type ModuleKey = 'chat' | 'forecast'

export interface ModuleDef {
  key: ModuleKey
  path: string
  navLabel: string
  headerTitle: string
  subtitle: string
}

export const MODULES: ModuleDef[] = [
  {
    key: 'chat',
    path: '/chat',
    navLabel: 'Ask the data',
    headerTitle: 'Ask the data',
    subtitle:
      'Ask questions about onboard sales in plain language. The agent queries the warehouse and answers with numbers, tables and charts.',
  },
  {
    key: 'forecast',
    path: '/forecast',
    navLabel: 'Fresh food forecast',
    headerTitle: 'Fresh food forecast',
    subtitle:
      'Enter the flight brief, choose an objective, and the model returns the recommended quantity per item.',
  },
]

export function moduleForPath(pathname: string): ModuleDef {
  return MODULES.find((m) => pathname.startsWith(m.path)) ?? MODULES[0]
}
