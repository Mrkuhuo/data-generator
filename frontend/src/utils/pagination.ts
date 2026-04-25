export function pageCount(totalItems: number, pageSize: number) {
  const safePageSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.floor(pageSize) : 1;
  return Math.max(1, Math.ceil(Math.max(totalItems, 0) / safePageSize));
}

export function clampPage(page: number, totalItems: number, pageSize: number) {
  const totalPages = pageCount(totalItems, pageSize);
  if (!Number.isFinite(page) || page < 1) {
    return 1;
  }
  return Math.min(Math.floor(page), totalPages);
}

export function paginateItems<T>(items: T[], page: number, pageSize: number) {
  const safePageSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.floor(pageSize) : 1;
  const safePage = clampPage(page, items.length, safePageSize);
  const start = (safePage - 1) * safePageSize;
  return items.slice(start, start + safePageSize);
}
