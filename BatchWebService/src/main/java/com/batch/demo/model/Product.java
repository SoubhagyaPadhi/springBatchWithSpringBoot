package com.batch.demo.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.math.BigDecimal;

@XmlRootElement(name = "product")
@XmlAccessorType(XmlAccessType.FIELD)
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

	public Product(Integer productID, String productName, String productDesc, BigDecimal price, Integer unit) {
		super();
		this.productID = productID;
		this.productName = productName;
		ProductDesc = productDesc;
		this.price = price;
		this.unit = unit;
	}
	
	
}
