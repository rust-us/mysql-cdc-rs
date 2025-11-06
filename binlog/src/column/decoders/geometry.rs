use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;

/// Enhanced GEOMETRY decoder with spatial data support
pub struct GeometryDecoder;

impl GeometryDecoder {
    pub fn new() -> Self {
        Self
    }

    /// Parse Well-Known Binary (WKB) geometry format
    fn parse_wkb_geometry(&self, data: &[u8]) -> Result<GeometryValue, ReError> {
        if data.len() < 9 {
            return Err(ReError::String("Geometry data too short".to_string()));
        }

        let mut cursor = Cursor::new(data);
        
        // Read byte order (1 byte)
        let byte_order = cursor.read_u8()?;
        let is_little_endian = byte_order == 1;
        
        // Read geometry type (4 bytes)
        let geometry_type = if is_little_endian {
            cursor.read_u32::<LittleEndian>()?
        } else {
            cursor.read_u32::<byteorder::BigEndian>()?
        };

        match geometry_type {
            1 => self.parse_point(&mut cursor, is_little_endian),
            2 => self.parse_linestring(&mut cursor, is_little_endian),
            3 => self.parse_polygon(&mut cursor, is_little_endian),
            4 => self.parse_multipoint(&mut cursor, is_little_endian),
            5 => self.parse_multilinestring(&mut cursor, is_little_endian),
            6 => self.parse_multipolygon(&mut cursor, is_little_endian),
            7 => self.parse_geometrycollection(&mut cursor, is_little_endian),
            _ => Err(ReError::String(format!("Unsupported geometry type: {}", geometry_type))),
        }
    }

    fn parse_point(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<GeometryValue, ReError> {
        let x = self.read_f64(cursor, is_little_endian)?;
        let y = self.read_f64(cursor, is_little_endian)?;
        
        Ok(GeometryValue::Point { x, y })
    }

    fn parse_linestring(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<GeometryValue, ReError> {
        let num_points = self.read_u32(cursor, is_little_endian)?;
        let mut points = Vec::new();
        
        for _ in 0..num_points {
            let x = self.read_f64(cursor, is_little_endian)?;
            let y = self.read_f64(cursor, is_little_endian)?;
            points.push(Point { x, y });
        }
        
        Ok(GeometryValue::LineString { points })
    }

    fn parse_polygon(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<GeometryValue, ReError> {
        let num_rings = self.read_u32(cursor, is_little_endian)?;
        let mut rings = Vec::new();
        
        for _ in 0..num_rings {
            let num_points = self.read_u32(cursor, is_little_endian)?;
            let mut points = Vec::new();
            
            for _ in 0..num_points {
                let x = self.read_f64(cursor, is_little_endian)?;
                let y = self.read_f64(cursor, is_little_endian)?;
                points.push(Point { x, y });
            }
            
            rings.push(points);
        }
        
        Ok(GeometryValue::Polygon { rings })
    }

    fn parse_multipoint(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<GeometryValue, ReError> {
        let num_points = self.read_u32(cursor, is_little_endian)?;
        let mut points = Vec::new();
        
        for _ in 0..num_points {
            // Each point has its own WKB header
            let _byte_order = cursor.read_u8()?;
            let _geometry_type = self.read_u32(cursor, is_little_endian)?;
            
            let x = self.read_f64(cursor, is_little_endian)?;
            let y = self.read_f64(cursor, is_little_endian)?;
            points.push(Point { x, y });
        }
        
        Ok(GeometryValue::MultiPoint { points })
    }

    fn parse_multilinestring(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<GeometryValue, ReError> {
        let num_linestrings = self.read_u32(cursor, is_little_endian)?;
        let mut linestrings = Vec::new();
        
        for _ in 0..num_linestrings {
            // Each linestring has its own WKB header
            let _byte_order = cursor.read_u8()?;
            let _geometry_type = self.read_u32(cursor, is_little_endian)?;
            
            let num_points = self.read_u32(cursor, is_little_endian)?;
            let mut points = Vec::new();
            
            for _ in 0..num_points {
                let x = self.read_f64(cursor, is_little_endian)?;
                let y = self.read_f64(cursor, is_little_endian)?;
                points.push(Point { x, y });
            }
            
            linestrings.push(points);
        }
        
        Ok(GeometryValue::MultiLineString { linestrings })
    }

    fn parse_multipolygon(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<GeometryValue, ReError> {
        let num_polygons = self.read_u32(cursor, is_little_endian)?;
        let mut polygons = Vec::new();
        
        for _ in 0..num_polygons {
            // Each polygon has its own WKB header
            let _byte_order = cursor.read_u8()?;
            let _geometry_type = self.read_u32(cursor, is_little_endian)?;
            
            let num_rings = self.read_u32(cursor, is_little_endian)?;
            let mut rings = Vec::new();
            
            for _ in 0..num_rings {
                let num_points = self.read_u32(cursor, is_little_endian)?;
                let mut points = Vec::new();
                
                for _ in 0..num_points {
                    let x = self.read_f64(cursor, is_little_endian)?;
                    let y = self.read_f64(cursor, is_little_endian)?;
                    points.push(Point { x, y });
                }
                
                rings.push(points);
            }
            
            polygons.push(rings);
        }
        
        Ok(GeometryValue::MultiPolygon { polygons })
    }

    fn parse_geometrycollection(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<GeometryValue, ReError> {
        let num_geometries = self.read_u32(cursor, is_little_endian)?;
        let mut geometries = Vec::new();
        
        for _ in 0..num_geometries {
            // Read the remaining data for this geometry
            let mut remaining_data = Vec::new();
            cursor.read_to_end(&mut remaining_data)?;
            
            // Parse the geometry recursively
            let geometry = self.parse_wkb_geometry(&remaining_data)?;
            geometries.push(Box::new(geometry));
        }
        
        Ok(GeometryValue::GeometryCollection { geometries })
    }

    fn read_u32(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<u32, ReError> {
        if is_little_endian {
            Ok(cursor.read_u32::<LittleEndian>()?)
        } else {
            Ok(cursor.read_u32::<byteorder::BigEndian>()?)
        }
    }

    fn read_f64(&self, cursor: &mut Cursor<&[u8]>, is_little_endian: bool) -> Result<f64, ReError> {
        if is_little_endian {
            Ok(cursor.read_f64::<LittleEndian>()?)
        } else {
            Ok(cursor.read_f64::<byteorder::BigEndian>()?)
        }
    }
}

impl TypeDecoder for GeometryDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as usize;
        let mut vec = vec![0; length];
        cursor.read_exact(&mut vec)?;
        
        // Try to parse as WKB geometry
        match self.parse_wkb_geometry(&vec) {
            Ok(geometry) => {
                // Convert to custom column value with structured geometry data
                let mut metadata_map = std::collections::HashMap::new();
                metadata_map.insert("geometry_type".to_string(), geometry.geometry_type().to_string());
                metadata_map.insert("wkt".to_string(), geometry.to_wkt());
                
                Ok(ColumnValue::Custom {
                    type_name: "GEOMETRY".to_string(),
                    data: vec,
                    metadata: metadata_map,
                })
            }
            Err(_) => {
                // Fallback: store as raw binary data
                Ok(ColumnValue::Geometry(vec))
            }
        }
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Geometry as u8
    }

    fn type_name(&self) -> &'static str {
        "GEOMETRY"
    }
}

/// Structured geometry value types
#[derive(Debug, Clone, PartialEq)]
pub enum GeometryValue {
    Point { x: f64, y: f64 },
    LineString { points: Vec<Point> },
    Polygon { rings: Vec<Vec<Point>> },
    MultiPoint { points: Vec<Point> },
    MultiLineString { linestrings: Vec<Vec<Point>> },
    MultiPolygon { polygons: Vec<Vec<Vec<Point>>> },
    GeometryCollection { geometries: Vec<Box<GeometryValue>> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl GeometryValue {
    /// Get the geometry type name
    pub fn geometry_type(&self) -> &'static str {
        match self {
            GeometryValue::Point { .. } => "POINT",
            GeometryValue::LineString { .. } => "LINESTRING",
            GeometryValue::Polygon { .. } => "POLYGON",
            GeometryValue::MultiPoint { .. } => "MULTIPOINT",
            GeometryValue::MultiLineString { .. } => "MULTILINESTRING",
            GeometryValue::MultiPolygon { .. } => "MULTIPOLYGON",
            GeometryValue::GeometryCollection { .. } => "GEOMETRYCOLLECTION",
        }
    }

    /// Convert to Well-Known Text (WKT) format
    pub fn to_wkt(&self) -> String {
        match self {
            GeometryValue::Point { x, y } => format!("POINT({} {})", x, y),
            GeometryValue::LineString { points } => {
                let coords: Vec<String> = points.iter()
                    .map(|p| format!("{} {}", p.x, p.y))
                    .collect();
                format!("LINESTRING({})", coords.join(", "))
            }
            GeometryValue::Polygon { rings } => {
                let ring_strs: Vec<String> = rings.iter()
                    .map(|ring| {
                        let coords: Vec<String> = ring.iter()
                            .map(|p| format!("{} {}", p.x, p.y))
                            .collect();
                        format!("({})", coords.join(", "))
                    })
                    .collect();
                format!("POLYGON({})", ring_strs.join(", "))
            }
            GeometryValue::MultiPoint { points } => {
                let point_strs: Vec<String> = points.iter()
                    .map(|p| format!("({} {})", p.x, p.y))
                    .collect();
                format!("MULTIPOINT({})", point_strs.join(", "))
            }
            GeometryValue::MultiLineString { linestrings } => {
                let line_strs: Vec<String> = linestrings.iter()
                    .map(|line| {
                        let coords: Vec<String> = line.iter()
                            .map(|p| format!("{} {}", p.x, p.y))
                            .collect();
                        format!("({})", coords.join(", "))
                    })
                    .collect();
                format!("MULTILINESTRING({})", line_strs.join(", "))
            }
            GeometryValue::MultiPolygon { polygons } => {
                let poly_strs: Vec<String> = polygons.iter()
                    .map(|poly| {
                        let ring_strs: Vec<String> = poly.iter()
                            .map(|ring| {
                                let coords: Vec<String> = ring.iter()
                                    .map(|p| format!("{} {}", p.x, p.y))
                                    .collect();
                                format!("({})", coords.join(", "))
                            })
                            .collect();
                        format!("({})", ring_strs.join(", "))
                    })
                    .collect();
                format!("MULTIPOLYGON({})", poly_strs.join(", "))
            }
            GeometryValue::GeometryCollection { geometries } => {
                let geom_strs: Vec<String> = geometries.iter()
                    .map(|geom| geom.to_wkt())
                    .collect();
                format!("GEOMETRYCOLLECTION({})", geom_strs.join(", "))
            }
        }
    }

    /// Calculate the bounding box of the geometry
    pub fn bounding_box(&self) -> Option<BoundingBox> {
        match self {
            GeometryValue::Point { x, y } => Some(BoundingBox {
                min_x: *x, max_x: *x,
                min_y: *y, max_y: *y,
            }),
            GeometryValue::LineString { points } => {
                if points.is_empty() {
                    return None;
                }
                let mut bbox = BoundingBox {
                    min_x: points[0].x, max_x: points[0].x,
                    min_y: points[0].y, max_y: points[0].y,
                };
                for point in points {
                    bbox.extend_point(point.x, point.y);
                }
                Some(bbox)
            }
            GeometryValue::Polygon { rings } => {
                if rings.is_empty() || rings[0].is_empty() {
                    return None;
                }
                let mut bbox = BoundingBox {
                    min_x: rings[0][0].x, max_x: rings[0][0].x,
                    min_y: rings[0][0].y, max_y: rings[0][0].y,
                };
                for ring in rings {
                    for point in ring {
                        bbox.extend_point(point.x, point.y);
                    }
                }
                Some(bbox)
            }
            // For multi-geometries, calculate the union of all bounding boxes
            _ => {
                // Simplified implementation
                None
            }
        }
    }

    /// Check if a point is contained within this geometry (simplified implementation)
    pub fn contains_point(&self, x: f64, y: f64) -> bool {
        match self {
            GeometryValue::Point { x: px, y: py } => {
                const EPSILON: f64 = 1e-10;
                (x - px).abs() < EPSILON && (y - py).abs() < EPSILON
            }
            GeometryValue::Polygon { rings } => {
                if rings.is_empty() {
                    return false;
                }
                // Use ray casting algorithm for the exterior ring
                self.point_in_polygon(x, y, &rings[0])
            }
            _ => false, // Simplified - other geometries would need more complex algorithms
        }
    }

    fn point_in_polygon(&self, x: f64, y: f64, polygon: &[Point]) -> bool {
        let mut inside = false;
        let mut j = polygon.len() - 1;
        
        for i in 0..polygon.len() {
            let xi = polygon[i].x;
            let yi = polygon[i].y;
            let xj = polygon[j].x;
            let yj = polygon[j].y;
            
            if ((yi > y) != (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi) {
                inside = !inside;
            }
            j = i;
        }
        
        inside
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundingBox {
    pub min_x: f64,
    pub max_x: f64,
    pub min_y: f64,
    pub max_y: f64,
}

impl BoundingBox {
    pub fn extend_point(&mut self, x: f64, y: f64) {
        self.min_x = self.min_x.min(x);
        self.max_x = self.max_x.max(x);
        self.min_y = self.min_y.min(y);
        self.max_y = self.max_y.max(y);
    }

    pub fn width(&self) -> f64 {
        self.max_x - self.min_x
    }

    pub fn height(&self) -> f64 {
        self.max_y - self.min_y
    }

    pub fn area(&self) -> f64 {
        self.width() * self.height()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_geometry() {
        let point = GeometryValue::Point { x: 1.0, y: 2.0 };
        assert_eq!(point.geometry_type(), "POINT");
        assert_eq!(point.to_wkt(), "POINT(1 2)");
        assert!(point.contains_point(1.0, 2.0));
        assert!(!point.contains_point(1.1, 2.0));
    }

    #[test]
    fn test_linestring_geometry() {
        let linestring = GeometryValue::LineString {
            points: vec![
                Point { x: 0.0, y: 0.0 },
                Point { x: 1.0, y: 1.0 },
                Point { x: 2.0, y: 0.0 },
            ],
        };
        assert_eq!(linestring.geometry_type(), "LINESTRING");
        assert_eq!(linestring.to_wkt(), "LINESTRING(0 0, 1 1, 2 0)");
        
        let bbox = linestring.bounding_box().unwrap();
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 2.0);
        assert_eq!(bbox.min_y, 0.0);
        assert_eq!(bbox.max_y, 1.0);
    }

    #[test]
    fn test_polygon_geometry() {
        let polygon = GeometryValue::Polygon {
            rings: vec![vec![
                Point { x: 0.0, y: 0.0 },
                Point { x: 1.0, y: 0.0 },
                Point { x: 1.0, y: 1.0 },
                Point { x: 0.0, y: 1.0 },
                Point { x: 0.0, y: 0.0 }, // Closed ring
            ]],
        };
        
        assert_eq!(polygon.geometry_type(), "POLYGON");
        assert!(polygon.contains_point(0.5, 0.5)); // Inside
        assert!(!polygon.contains_point(1.5, 0.5)); // Outside
    }
}