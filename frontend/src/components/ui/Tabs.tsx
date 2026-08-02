import type { ComponentProps, ReactNode } from 'react';
import {
  Tab as AriaTab,
  TabList as AriaTabList,
  TabPanel as AriaTabPanel,
  Tabs as AriaTabs,
  type Key,
} from 'react-aria-components';

export function Tabs({
  selectedKey,
  onSelectionChange,
  children,
  className,
  orientation = 'horizontal',
}: {
  selectedKey: Key;
  onSelectionChange: (key: Key) => void;
  children: ReactNode;
  className?: string;
  orientation?: 'horizontal' | 'vertical';
}) {
  return (
    <AriaTabs
      selectedKey={selectedKey}
      onSelectionChange={onSelectionChange}
      keyboardActivation="automatic"
      orientation={orientation}
      className={className}
    >
      {children}
    </AriaTabs>
  );
}

export function TabList(props: ComponentProps<typeof AriaTabList>) {
  return <AriaTabList {...props} />;
}

export function Tab(props: ComponentProps<typeof AriaTab>) {
  return <AriaTab {...props} />;
}

export function TabPanel(props: ComponentProps<typeof AriaTabPanel>) {
  return <AriaTabPanel {...props} />;
}
