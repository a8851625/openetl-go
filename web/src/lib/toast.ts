import { toast as sonnerToast } from 'sonner';

export type ToastType = 'success' | 'error' | 'info';

/** 统一 toast：走 sonner，兼容旧 toast(type, msg) 调用签名 */
export function showToast(type: ToastType, msg: string) {
  if (type === 'success') sonnerToast.success(msg);
  else if (type === 'error') sonnerToast.error(msg);
  else sonnerToast.info(msg);
}

export type ToastFn = (type: ToastType, msg: string) => void;
