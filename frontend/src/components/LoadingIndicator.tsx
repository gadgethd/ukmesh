import React from 'react';

type LoadingIndicatorVariant = 'block' | 'inline' | 'overlay';

type LoadingIndicatorProps = {
  label?: string;
  variant?: LoadingIndicatorVariant;
  className?: string;
};

export const LoadingIndicator: React.FC<LoadingIndicatorProps> = ({
  label = 'Loading...',
  variant = 'block',
  className = '',
}) => {
  const cls = ['loading-indicator', `loading-indicator--${variant}`, className]
    .filter(Boolean)
    .join(' ');
  const Tag = variant === 'inline' ? 'span' : 'div';

  return (
    <Tag className={cls} role="status" aria-live="polite">
      <span className="loading-indicator__mark" aria-hidden="true" />
      <span className="loading-indicator__label">{label}</span>
    </Tag>
  );
};
