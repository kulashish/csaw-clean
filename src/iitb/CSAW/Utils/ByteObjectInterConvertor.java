package iitb.CSAW.Utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ByteObjectInterConvertor {

	public static int SHORT_SIZE = Short.SIZE/Byte.SIZE;
	public static int INTEGER_SIZE = Integer.SIZE/Byte.SIZE;
	public static int DOUBLE_SIZE = Double.SIZE/Byte.SIZE;
	
	public static byte[] intToByteArray(int d) {
		byte[] dword = new byte[INTEGER_SIZE];
		for(int i=0; i<INTEGER_SIZE; i++)
			dword[i] = (byte) ((d >> (Byte.SIZE*i)) & 0x00FF);
		return dword;
	}
	public static int byteArrayToInt(byte[] b) {

		int x = 0;
		int musk = 0x00FF;
		for(int i=0; i<INTEGER_SIZE; i++) {
			int t = (musk & b[i]); 
			t <<= (i*8);
			x |= t;
		}
		return x;
	}
	public static byte[] shortToByteArray(short s)  {
		byte[] dword = new byte[SHORT_SIZE];
		for(int i=0; i<SHORT_SIZE; i++)
			dword[i] = (byte) ((s >> (Byte.SIZE*i)) & 0x00FF);
		return dword;
	}
	public static short byteArrayToShort(byte[] b) {

		short x = 0;
		int musk = 0x00FF;
		for(int i=0; i<SHORT_SIZE; i++) {
			int t = (musk & b[i]); 
			t <<= (i*8);
			x |= t;
		}
		return x;
	}
	public static byte[] doubleToByteArray(double d) 
	throws IOException {
		ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
		DataOutputStream datastream = new DataOutputStream(bytestream);
		datastream.writeDouble(d);
		datastream.flush();
		byte[] b= bytestream.toByteArray();
		bytestream.close();
		datastream.close();
		return b;
	}
	public static double byteArrayToDouble(byte[] b_array) throws IOException {
		ByteArrayInputStream bytestream = new ByteArrayInputStream(b_array);
		DataInputStream ds = new DataInputStream(bytestream);
		double d = ds.readDouble();
		ds.close();
		bytestream.close();
		return d;
	}

	public static byte[] toBytes(Object object) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
		} catch (java.io.IOException ioe) {
			ioe.printStackTrace();
		}
		return baos.toByteArray();
	}

	public static Object toObject(byte[] bytes) {
		Object object = null;
		try {
			object = new ObjectInputStream(new ByteArrayInputStream(bytes))
					.readObject();
		} catch (IOException ioe) {

			ioe.printStackTrace();
		} catch (java.lang.ClassNotFoundException cnfe) {
			cnfe.printStackTrace();
		}
		return object;
	}
	
	public static void main(String[] args) 
	throws IOException{
		int d = Integer.valueOf(args[0]);
		byte[] b = intToByteArray(d);
		d = byteArrayToInt(b);
		System.out.println(d);
	}
}
