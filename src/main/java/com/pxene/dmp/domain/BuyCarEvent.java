package com.pxene.dmp.domain;

/**
 * 描述用户的一次购买汽车行为,对应buy_cars里的一个json对象
 * 
 * @author xuliuming
 *
 */
public class BuyCarEvent {
	private String time;
	private String address;
	private String price;
	private String car;

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	public String getCar() {
		return car;
	}

	public void setCar(String car) {
		this.car = car;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BuyCarEvent) {
			return this.car.equals(((BuyCarEvent) obj).car);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.car.hashCode();
	}
}
