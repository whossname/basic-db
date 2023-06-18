pub mod table_leaf;
pub mod table_interior;

#[derive(Debug)]
pub enum PageType {
    TableLeaf(table_leaf::TableLeaf),
    TableInterior(table_interior::TableInterior),
    IndexLeaf,
    IndexInterior,
}

#[derive(Debug)]
pub struct Page {
    pub data: Vec<u8>,
    pub page_type: PageType,
}
