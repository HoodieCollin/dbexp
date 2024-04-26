fn main() -> anyhow::Result<()> {
    // const N: usize = 10;

    // let c1 = DataType::O32;
    // let mut c1_buf = vec![0; c1.byte_count() * N];
    // let c1_values = vec![
    //     DataValue::O32(oid::O32::from_uint(1u32)),
    //     DataValue::O32(oid::O32::from_uint(2u32)),
    //     DataValue::O32(oid::O32::from_uint(3u32)),
    //     DataValue::O32(oid::O32::from_uint(4u32)),
    //     DataValue::O32(oid::O32::from_uint(5u32)),
    //     DataValue::O32(oid::O32::from_uint(6u32)),
    //     DataValue::O32(oid::O32::from_uint(7u32)),
    //     DataValue::O32(oid::O32::from_uint(8u32)),
    //     DataValue::O32(oid::O32::from_uint(9u32)),
    //     DataValue::O32(oid::O32::from_uint(10u32)),
    // ];

    // let mut c1_unloader = Unloader::new(&mut c1_buf, &c1_values);

    // c1_unloader.all()?;

    // let c2 = DataType::Bool;
    // let mut c2_buf = vec![0; c2.byte_count() * N];
    // let c2_values = vec![
    //     DataValue::Bool(true),
    //     DataValue::Bool(false),
    //     DataValue::Bool(true),
    //     DataValue::Bool(false),
    //     DataValue::Bool(true),
    //     DataValue::Bool(false),
    //     DataValue::Bool(true),
    //     DataValue::Bool(false),
    //     DataValue::Bool(true),
    //     DataValue::Bool(false),
    // ];

    // let mut c2_unloader = Unloader::new(&mut c2_buf, &c2_values);

    // c2_unloader.all()?;

    // // let c3 = DataType::Text(128);
    // let c3 = DataType::Integer(IntSize::X16);
    // let mut c3_buf = vec![0; c3.byte_count() * N];
    // let c3_values = vec![
    //     DataValue::try_from_any(c3, 100)?,
    //     DataValue::try_from_any(c3, 200)?,
    //     DataValue::try_from_any(c3, 300)?,
    //     DataValue::try_from_any(c3, 400)?,
    //     DataValue::try_from_any(c3, 500)?,
    //     DataValue::try_from_any(c3, 600)?,
    //     DataValue::try_from_any(c3, 700)?,
    //     DataValue::try_from_any(c3, 800)?,
    //     DataValue::try_from_any(c3, 900)?,
    //     DataValue::try_from_any(c3, 1000)?,
    // ];

    // let mut c3_unloader = Unloader::new(&mut c3_buf, &c3_values);

    // c3_unloader.all()?;

    // let t1 = system.create_table(vec![c1, c2, c3]);
    // let t_columns = t1.columns();
    // let mut t_iter = t_columns.iter().copied();
    // let (c1, c2, c3) = (
    //     t_iter.next().unwrap(),
    //     t_iter.next().unwrap(),
    //     t_iter.next().unwrap(),
    // );

    // // t1.load_data(vec![(c1, c1_buf), (c2, c2_buf), (c3, c3_buf)])?;

    // // let c1 = DataType::O64;
    // // let c2 = DataType::Integer(IntSize::X16);
    // // let c3 = DataType::Integer(IntSize::X16);
    // // let t2 = system.create_table(vec![c1, c2, c3]);

    // println!("{:#?}", system);

    Ok(())
}
