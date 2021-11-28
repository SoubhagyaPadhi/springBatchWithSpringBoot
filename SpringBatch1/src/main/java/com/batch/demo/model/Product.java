package com.batch.demo.model;

import java.math.BigDecimal;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="product")
public class Product {

	private Integer productID;
	private String productName;
	private String ProductDesc;
	private BigDecimal price;
	private Integer unit;

	public Integer getProductID() {
		return productID;
	}

	public void setProductID(Integer productID) {
		this.productID = productID;
	}

	@Override
	public String toString() {
		return "Product [productID=" + productID + ", productName=" + productName + ", ProductDesc=" + ProductDesc
				+ ", price=" + price + ", unit=" + unit + "]";
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public String getProductDesc() {
		return ProductDesc;
	}

	public void setProductDesc(String productDesc) {
		ProductDesc = productDesc;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	public Integer getUnit() {
		return unit;
	}

	public void setUnit(Integer unit) {
		this.unit = unit;
	}

}
