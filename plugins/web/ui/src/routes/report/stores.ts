import { writable, type Writable } from 'svelte/store';

type MarkdownBlockType = {
  category: 'markdown';
  params: {
    title: string;
    content: string;
    isEditing: boolean;
  };
};
type QueryBlockType = {
  category: 'query';
  params: {
    title: string;
    query: string;
    isEditing: boolean;
  };
};

export type Report = {
  title: string;
  blocks: (MarkdownBlockType | QueryBlockType)[];
};
export const report: Writable<Report> = writable({
  title: 'Untitled Report',
  blocks: []
});
