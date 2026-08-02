import { useEffect, useRef, type ReactNode } from 'react';
import {
  Button,
  Dialog as AriaDialog,
  Heading,
  Modal,
  ModalOverlay,
} from 'react-aria-components';

export type DialogProps = {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  ariaLabel?: string;
  className?: string;
  overlayClassName?: string;
  children: ReactNode | ((close: () => void) => ReactNode);
};

export function Dialog({
  isOpen,
  onOpenChange,
  ariaLabel,
  className,
  overlayClassName,
  children,
}: DialogProps) {
  const dialogRef = useRef<HTMLElement>(null);

  useEffect(() => {
    if (!isOpen) return undefined;
    const frame = requestAnimationFrame(() => {
      if (!dialogRef.current) return;
      dialogRef.current.tabIndex = -1;
      dialogRef.current.focus();
    });
    return () => cancelAnimationFrame(frame);
  }, [isOpen]);

  return (
    <ModalOverlay
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      isDismissable
      className={overlayClassName ?? 'ui-dialog-overlay'}
    >
      <Modal className="ui-dialog-modal">
        <AriaDialog
          ref={dialogRef}
          aria-label={ariaLabel}
          className={className ? `ui-dialog ${className}` : 'ui-dialog'}
        >
          {({ close }) => typeof children === 'function' ? children(close) : children}
        </AriaDialog>
      </Modal>
    </ModalOverlay>
  );
}

export function DialogTitle({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return <Heading slot="title" className={className}>{children}</Heading>;
}

export function DialogCloseButton({
  children = 'Close',
  className,
}: {
  children?: ReactNode;
  className?: string;
}) {
  return (
    <Button slot="close" className={className}>
      {children}
    </Button>
  );
}
