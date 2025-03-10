/*!
  This module contains types for parse responses.
*/

use std::{
  collections::HashMap,
  io::{self, Cursor, Read},
  marker::PhantomData,
};

use super::{constants::{Code, Field}, types::Error};

use num_traits::FromPrimitive;
use rmp::decode::{read_array_len, read_int, read_map_len};
use rmpv::{Value, decode::read_value};
use serde::de::DeserializeOwned;

/// This is representation of tarantool response.
#[derive(Debug, Clone)]
pub struct Response {
  pub header: Header,
  pub body: Option<Vec<u8>>,
}

#[allow(dead_code)]
impl Response {
  /// allows you to parse iproto response header.
  pub fn parse<R>(mut reader: R) -> Result<Self, Error>
    where R: Read
  {
    let size: u64 = read_int(&mut reader)?;
    let mut reader = reader.take(size);

    let header = Header::unpack(&mut  reader)?;

    let mut body: Vec<u8> = Vec::with_capacity(size as usize);

    reader.read_to_end(&mut body)?;

    if body.is_empty() {
      return Ok(Response { header, body: None });
    }

    body.shrink_to_fit();
    //print!("header:{:#?}", header);
    Ok(Response { header, body: Some(body) })
  }

  /// allows you to parse response body.
  pub fn unpack_body<B>(&self) -> Result<B::Result, Error>
    where B: BodyDecoder,
  {
    match &self.body {
      Some(body) => {
        //print!("body:{:?}", &self.body); 
        B::unpack(body)
      },
      None => Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "body is empty",
      ).into()),
    }
  }

  /// allows you to parse response body.
  pub fn unpack_body_from_execute_select<B>(&self) -> Result<B::Result, Error>
    where B: BodyDecoder,
  {
    match &self.body {
      Some(body) => {
        //print!("body:{:?}", body); 
        B::unpack(body)
      },
      None => Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "body is empty",
      ).into()),
    }
  }
}

/// Representation of response header.
#[derive(Debug, Default, Clone, Copy)]
pub struct Header {
  pub code: Code,
  pub sync: u64,
  pub schema: u64,
}

#[allow(dead_code)]
impl Header {
  fn unpack<R>(reader: &mut R) -> Result<Self, Error>
    where R: Read,
  {
    let mut header = Header::default();

    for _ in 0..read_map_len(reader)? {
      let raw_field: u64 = read_int(reader)?;
      let field: Field = FromPrimitive::from_u64(raw_field)
        .ok_or(Error::UnexpectedField(raw_field))?;

      match field {
        Field::RequestType => {
          let value = read_int(reader)?;
          header.code = FromPrimitive::from_u64(value)
            .ok_or(Error::UnexpectedValue(field))?;
        },
        Field::Sync => { header.sync = read_int(reader)? },
        Field::SchemaVersion => { header.schema = read_int(reader)? },
        _ => {
          log::debug!("skipping value due to unexpected field {:?}", field);
          read_value(reader)?;
        },
      }
    }

    Ok(header)
  }
}

/**
  This trait is used for parsing response body.

  If you want to parse custom body you should implement it.
*/
pub trait BodyDecoder {
  type Result;
  fn unpack(body: &[u8]) -> Result<Self::Result, Error>;
}

#[derive(Debug, Default, Clone)]
pub struct StackRecord {
  pub err_type: String,
  pub file: String,
  pub line: u64,
  pub message: String,
  pub errno: u64,
  pub errcode: u64,
}


/// This is representation of error returned from tarantool.
#[derive(Debug, Clone)]
pub struct TarantoolError {
  pub message: String,
  pub stack: Vec<StackRecord>,
}

/// This is decoder for error body.
pub struct ErrorBody;

impl BodyDecoder for ErrorBody {
  type Result = TarantoolError;

  fn unpack(body: &[u8]) -> Result<Self::Result, Error> {
    let mut reader = Cursor::new(body);
    let reader = &mut reader;

    let map_len = read_map_len(reader)?;

    let mut body = TarantoolError {
      message: String::new(), stack: Vec::new(),
    };

    let read_string = |reader: &mut Cursor<&[u8]>| -> Result<String, Error> {
      let str_len = rmp::decode::read_str_len(reader)?;
      let mut buf: Vec<u8> = Vec::new();
      buf.resize(str_len as usize, 0);
      reader.read_exact(&mut buf)?;
      String::from_utf8(buf).map_err(|_| io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid ut8 string",
      ).into())
    };

    for _ in 0..map_len {
      let raw_field: u64 = read_int(reader)?;
      let field: Field = FromPrimitive::from_u64(raw_field)
        .ok_or(Error::UnexpectedField(raw_field))?;

      match field {
        Field::Error24 => { body.message = read_string(reader)? },

        // see more here
        // https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#msgpack-ext-error
        Field::Error => {
          let map_len = read_map_len(reader)?;
          for _ in 0..map_len {
            if read_int::<u64, _>(reader)? != 0 { // field stack
              read_value(reader)?;
              continue;
            }

            let stack_len = read_array_len(reader)?;

            let mut stack: Vec<StackRecord> = Vec::with_capacity(stack_len as usize);

            for _ in 0..stack_len {
              let mut stack_record = StackRecord::default();

              for _ in 0..read_map_len(reader)? {
                match read_int::<u64, _>(reader)? {
                  0 => { stack_record.err_type = read_string(reader)?; },
                  1 => { stack_record.file = read_string(reader)?; },
                  2 => { stack_record.line = read_int(reader)?; },
                  3 => { stack_record.message = read_string(reader)?; },
                  4 => { stack_record.errno = read_int(reader)?; }
                  5 => { stack_record.errcode = read_int(reader)?; }
                  _ => { read_value(reader)?; },
                }
              }

              stack.push(stack_record);
            }

            body.stack = stack;
          }
        },

        _ => {
          log::debug!("skipping value due to unexpected field {:?}", field);
          read_value(reader)?;
        },
      };
    }

    Ok(body)
  }
}

/// This is default decoder for response body.
pub struct TupleBody<T>(PhantomData<T>)
  where T: DeserializeOwned;

impl<T> BodyDecoder for TupleBody<T>
  where T: DeserializeOwned
{
  type Result = T;

  fn unpack(body: &[u8]) -> Result<T, Error> {
    
    let mut cur = Cursor::new(body);
    
    let map_len = read_map_len(&mut cur)?;
    //println!("map_len: {:#?}",map_len );
    if map_len != 1 {
      return Err(io::Error::new(
        io::ErrorKind::Other, "expected 1 field",
      ).into());
    }

    let raw_field: u64 = read_int(&mut cur)?;
    let field: Field = FromPrimitive::from_u64(raw_field)
      .ok_or(Error::UnexpectedField(raw_field))?;
     //println!("field:{:?} ", cur);
    
    match field {
      Field::Data =>
        rmp_serde::decode::from_read::<_, T>(cur)
          .map_err(Error::ParseError),
      _ => Err(Error::UnexpectedField(raw_field)),
    }
  }
}

/// This is default decoder for response body from Execute Select SQL.
pub struct TupleBodySelect<T>(PhantomData<T>)
  where T: DeserializeOwned;

impl<T> BodyDecoder for TupleBodySelect<T>
  where T: DeserializeOwned
{
  type Result = T;

  fn unpack(body: &[u8]) -> Result<T, Error> {
    let mut cur = Cursor::new(body);
    let cur = &mut cur;
    

    let map_len = read_map_len(cur)?;
    //println!("Map length: {:#?}", map_len);

    if map_len != 2 {
        return Err(io::Error::new(
            io::ErrorKind::Other, "expected 2 fields",
        ).into());
    }

    let mut field_data: Option<T> = None;

    for _ in 0..map_len {
      //println!("Iterate: {:#?}", ln);
      //print!("cur: {:?}", cur);
      //println!("Map length: {:#?}", map_len);
      let raw_field: u64 = read_int(cur)?;
      //println!("Raw field value: {:#?}", raw_field);
      
      let field: Field = FromPrimitive::from_u64(raw_field)
          .ok_or(Error::UnexpectedField(raw_field))?;
      
      match field {
          Field::Data => {
                //let value = read_value(cur)?;
                //println!("Field: {:#?}", field);
                field_data = Some(rmp_serde::decode::from_read::<_, T>(cur.by_ref())
                  .map_err(Error::ParseError)?);
          }
          _ => {
            #[warn(unused_must_use)]
            read_value(cur)?; //This require move cur to next position
          }
      }
      //println!("Cursor position after iteration: {:#?}", cur.position());
  }

    match field_data {
        Some(data) => Ok(data),
        None => Err(Error::UnexpectedField(0)),
    }
}


}

/// This is representation of SQL response body.
pub type SQLBody = HashMap<Field, Value>;


/// Decoder for SQL body.
pub struct SQLBodyDecoder;

impl BodyDecoder for SQLBodyDecoder {
  type Result = SQLBody;

  fn unpack(body: &[u8]) -> Result<Self::Result, Error> {
    let mut reader = Cursor::new(body);
    let reader = &mut reader;

    let mut body = HashMap::new();

    let map_len = read_map_len(reader)?;
    
    for _ in 0..map_len {
      let raw_field: u64 = read_int(reader)?;
      let field: Field = FromPrimitive::from_u64(raw_field)
        .ok_or(Error::UnexpectedField(raw_field))?;

        // println!("sql_test reader: {:#?} ", reader);
        // println!("sql_test raw_field: {:#?} ", raw_field);
        // println!("sql_test field: {:#?} ", field);
        // println!("sql_test read_value: {:#?} ", field);
        // let test =  rmp_serde::decode::from_read::<_, Decimal>(reader.clone())
        //         .map_err(Error::ParseError);
        //       println!("sql_test cur: {:#?}", test);

      let value = read_value(reader)?;
     
      body.insert(field, value);
    }
    
    Ok(body)
  }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_body() {
      let buf = [
        206, 0, 0, 0, 34, 131, 0, 206, 0, 0, 0, 0, 1,
        207, 0, 0, 0, 0, 0, 0, 0, 0, 5, 206, 0, 0, 0,
        80, 129, 48, 221, 0, 0, 0, 1, 147, 1, 2, 3,
      ];
      let resp = Response::parse(&buf[..]).unwrap();
      let tuple: Vec<(u64, u64, u64)> = resp.unpack_body::<TupleBody<_>>().unwrap();
      assert_eq!(&tuple, &[(1, 2, 3)]);
    }


    #[test]
    fn test_call_body() {
      let buf = [
        206, 0, 0, 0, 32, 131, 0, 206, 0, 0, 0, 0, 1, 207,
        0, 0, 0, 1, 0, 0, 0, 99, 5, 206, 0, 0, 0, 80,
        129, 48, 221, 0, 0, 0, 2, 123, 124,
      ];

      let resp = Response::parse(&buf[..]).unwrap();
      let tuple: (u64, u64) = resp.unpack_body::<TupleBody<_>>().unwrap();
      assert_eq!(tuple, (123, 124));
    }

    #[test]
    fn test_error_body() {

      let buf = [
        206, 0, 0, 0, 147, // len
        131, 0, 206, 0, 0, 128, 20, 1, 207, 0, 0, 0, 0, 0, 0, 0, 0, 5, 206, 0, 0, 0, 80, // header
        130, 49, 189, 73, 110, 118, 97, 108, 105, 100, 32, 77, 115, 103, 80, 97, 99,
        107, 32, 45, 32, 112, 97, 99, 107, 101, 116, 32, 98, 111, 100, 121, 82, 129,
        0, 145, 134, 0, 171, 67, 108, 105, 101, 110, 116, 69, 114, 114, 111, 114, 2,
        204, 216, 1, 217, 33, 47, 117, 115, 114, 47, 115, 114, 99, 47, 116, 97, 114,
        97, 110, 116, 111, 111, 108, 47, 115, 114, 99, 47, 98, 111, 120, 47, 120, 114,
        111, 119, 46, 99, 3, 189, 73, 110, 118, 97, 108, 105, 100, 32, 77, 115, 103,
        80, 97, 99, 107, 32, 45, 32, 112, 97, 99, 107, 101, 116, 32, 98, 111, 100, 121, 4, 0, 5, 20,
      ];

      let resp = Response::parse(&buf[..]).unwrap();

      assert!(resp.header.code.is_err());

      let err = resp.unpack_body::<ErrorBody>().unwrap();
      assert_eq!(err.message, "Invalid MsgPack - packet body");
      assert_eq!(err.stack.len(), 1);
    }
}
