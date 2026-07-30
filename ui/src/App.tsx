import { Navigate, Route, Routes } from 'react-router-dom'
import { Layout } from './components/Layout'
import { ChatView } from './views/ChatView'
import { DashboardsView } from './views/DashboardsView'
import { ForecastingView } from './views/ForecastingView'

function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Navigate to="/dashboards" replace />} />
        <Route path="/dashboards" element={<DashboardsView />} />
        <Route path="/chat" element={<ChatView />} />
        <Route path="/forecasting" element={<ForecastingView />} />
      </Route>
    </Routes>
  )
}

export default App
