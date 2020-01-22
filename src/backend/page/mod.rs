pub mod table_leaf;

pub struct TableInterior {
    freeblock_index: u16,
    cell_count: u16,
    cell_content_start: u16,
    fragmented_bytes_count: u8,
    right_pointer: u32,
}

pub enum PageType {
    TableLeaf(table_leaf::TableLeaf),
    TableInterior(TableInterior),
    IndexLeaf,
    IndexInterior,
}

pub struct Page {
    pub page: Vec<u8>,
    pub page_type: PageType,
}
