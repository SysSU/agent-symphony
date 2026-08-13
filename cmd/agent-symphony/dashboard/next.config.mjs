/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "export",
  generateBuildId: async () => "agent-symphony",
};

export default nextConfig;
