import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import { ThemeContext } from "app/contexts/theme/context";
import { defaultTheme } from "configs/theme.config";

const testThemeValue = {
  ...defaultTheme,
  isDark: false,
  setThemeMode: () => {},
  setThemeLayout: () => {},
  toggleMonochromeMode: () => {},
  setLightColorScheme: () => {},
  setDarkColorScheme: () => {},
  setPrimaryColorScheme: () => {},
  setNotificationPosition: () => {},
  setNotificationExpand: () => {},
  setCardSkin: () => {},
  resetTheme: () => {},
};

export function renderWithProviders(ui) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        refetchInterval: false,
      },
      mutations: {
        retry: false,
      },
    },
  });

  return {
    queryClient,
    ...render(
      <QueryClientProvider client={queryClient}>
        <ThemeContext value={testThemeValue}>{ui}</ThemeContext>
      </QueryClientProvider>,
    ),
  };
}
