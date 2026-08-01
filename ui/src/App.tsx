import { Navigate, Route, Routes } from 'react-router-dom'
import { Layout } from './components/Layout'
import { ChatView } from './views/ChatView'
import { ForecastingView } from './views/ForecastingView'

function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Navigate to="/chat" replace />} />
        <Route path="/chat" element={<ChatView />} />
        <Route path="/forecast" element={<ForecastingView />} />
      </Route>
    </Routes>
  )
}

export default App
