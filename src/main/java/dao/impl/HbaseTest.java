package dao.impl;

import dao.HBaseDAO;

public class HbaseTest {
	public static void main(String[] args) {
		
		
		HBaseDAOImp dao=new HBaseDAOImp();
		String[] cfs={"cf1"};
//		dao.createTable("ee_car", cfs);
//		dao.deleteTable("ee_car");
//		dao.scaner("ee_car");
//		dao.insert("ee_car", "1234", "cf1", "name", "23");
		dao.insert("ee_car", "1234", "cf1", "age", "23");
//		dao.insert("ee_car", "12345", "cf1", "name", "23");
	}
}
