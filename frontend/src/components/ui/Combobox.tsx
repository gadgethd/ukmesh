import { useRef, type ReactNode, type RefObject } from 'react';
import {
  Button,
  ComboBox as AriaComboBox,
  Input,
  Label,
  ListBox,
  ListBoxItem,
  Popover,
  type Key,
} from 'react-aria-components';

export type ComboboxOption = {
  id: string;
  label: string;
  description?: string;
  content?: ReactNode;
};

export type ComboboxProps = {
  label: string;
  value: string;
  onValueChange: (value: string) => void;
  onSelectionChange: (id: string) => void;
  options: ComboboxOption[];
  placeholder?: string;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  inputRef?: RefObject<HTMLInputElement | null>;
  autoFocus?: boolean;
  emptyContent?: ReactNode;
  footer?: ReactNode;
  className?: string;
  inputClassName?: string;
  popoverClassName?: string;
  optionClassName?: string;
  showToggleButton?: boolean;
};

export function Combobox({
  label,
  value,
  onValueChange,
  onSelectionChange,
  options,
  placeholder,
  isOpen,
  onOpenChange,
  inputRef,
  autoFocus,
  emptyContent,
  footer,
  className,
  inputClassName,
  popoverClassName,
  optionClassName,
  showToggleButton = false,
}: ComboboxProps) {
  const selectionInProgressRef = useRef(false);

  return (
    <AriaComboBox<ComboboxOption>
      className={className}
      inputValue={value}
      onInputChange={(nextValue) => {
        if (!selectionInProgressRef.current) onValueChange(nextValue);
      }}
      onSelectionChange={(key: Key | null) => {
        if (key == null) return;
        selectionInProgressRef.current = true;
        onSelectionChange(String(key));
        queueMicrotask(() => {
          selectionInProgressRef.current = false;
          onOpenChange(false);
        });
      }}
      items={options}
      menuTrigger="input"
      allowsCustomValue
      allowsEmptyCollection
    >
      <Label className="ui-visually-hidden">{label}</Label>
      <div className="ui-combobox__input-wrap">
        <Input
          ref={inputRef}
          className={inputClassName}
          placeholder={placeholder}
          autoFocus={autoFocus}
        />
        {showToggleButton ? (
          <Button className="ui-combobox__toggle" aria-label={`Show ${label} options`}>⌄</Button>
        ) : null}
      </div>
      <Popover
        isOpen={isOpen}
        onOpenChange={onOpenChange}
        isNonModal
        className={popoverClassName ?? 'ui-combobox__popover'}
      >
        <ListBox<ComboboxOption>
          className="ui-combobox__listbox"
          renderEmptyState={() => <div className="ui-combobox__empty">{emptyContent ?? 'No options'}</div>}
        >
          {(option) => (
            <ListBoxItem
              id={option.id}
              textValue={option.label}
              className={optionClassName}
            >
              {option.content ?? (
                <>
                  <span>{option.label}</span>
                  {option.description ? <small>{option.description}</small> : null}
                </>
              )}
            </ListBoxItem>
          )}
        </ListBox>
        {footer}
      </Popover>
    </AriaComboBox>
  );
}
