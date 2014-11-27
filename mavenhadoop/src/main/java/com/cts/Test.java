package com.cts;

public class Test {
	public static void main(String args[])
	{
		String name="khaja,asmath,,hai,,mohammed";
		String nam[]=name.split(",");
		for(String na:nam)
		{
			System.out.println("value-->"+na);
		}
	}

}
