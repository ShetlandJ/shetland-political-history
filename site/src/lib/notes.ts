// Link helpers for the Lerwick notes pages (src/pages/notes/).

export type Src = { label: string; href: string };

const bna = (code: string) => (ymd: string, page: number, article: number) =>
  `https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F${code}%2F${ymd}&page=${String(page).padStart(4, '0')}&article=${String(article).padStart(3, '0')}`;

/** Shetland Times article in the BNA viewer. */
export const st = bna('0000666');
/** Shetland News article in the BNA viewer. */
export const sn = bna('0003210');

export const bayanne = (id: string) => `https://www.bayanne.info/Shetland/getperson.php?personID=${id}&tree=ID1`;

/** Inline Bayanne link for HTML strings (rendered with set:html). */
export const b = (id: string, name: string) => `<a href="${bayanne(id)}" target="_blank" rel="noopener">${name}</a>`;
