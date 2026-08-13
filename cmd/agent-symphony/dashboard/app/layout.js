import "./styles.css";
import "@xterm/xterm/css/xterm.css";

export const metadata = {
  title: "Agent Symphony",
  description: "Repository orchestration status",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
