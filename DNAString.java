package bucket_sort;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Math;

public class DNAString {
	
	private byte[] dnaBytes;
	private int dnaBytesSize;
	private long dnaStringSize;

  private char[] decHashTable;
	
	private byte encodeBP(byte b) {
    char bp = (char) b;
		switch (bp) {
		case 'a':
		case 'A':
			return (byte) 0;
			
		case 'c':
		case 'C':
			return (byte) 0x01;
			
		case 'g':
		case 'G':
			return (byte) 0x02;
			
		case 't':
		case 'T':
			return (byte) 0x03;
			
		case 'n':
		case 'N':
			return (byte) 0x04;

    case '|':
      return (byte) 0x05;
		}
		
		// This should never occur.
		System.err.println("Invalid character found: " + bp);
		return 0;
	}
	
	private void encode(byte[] dnaString, int size, int pos, byte[] dnaBytes, int dnaBytesPos) {
		int encoded = 0;
		int shifted = 0;
		for (int i = pos; i < size && i < pos + 8; ++i) {
			byte b = encodeBP(dnaString[i]);
			encoded |= b;
			encoded <<= 3;
			shifted += 3;
		}
		encoded >>= 3;
		shifted -= 3;
		encoded <<= (21 - shifted);
		dnaBytes[dnaBytesPos] = (byte) ((encoded >> 16) & 0xFF);
		dnaBytes[dnaBytesPos+1] = (byte) ((encoded >> 8) & 0xFF);
		dnaBytes[dnaBytesPos + 2] = (byte) (encoded & 0xFF);
    }
	
  private void createDnaBytes(BufferedInputStream stream) {
    int j = 0;
    long readSoFar = 0;
    try {
      byte[] buffer = new byte[4096];
      int bytesRead = 0;
      while ((bytesRead = stream.read(buffer)) != -1) {
        readSoFar += bytesRead;
        if (readSoFar > dnaStringSize)
          bytesRead = bytesRead - (int) (readSoFar - dnaStringSize);
        for (int i = 0; i < bytesRead; i += 8) {
          encode(buffer, bytesRead, i, dnaBytes, j);
          j += 3;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
		
  public DNAString(BufferedInputStream stream, long size) {
    decHashTable = new char[6];
    decHashTable[0] = 'a';
    decHashTable[1] = 'c';
    decHashTable[2] = 'g';
    decHashTable[3] = 't';
    decHashTable[4] = 'n';
    decHashTable[5] = '|';
    dnaStringSize = size;
    dnaBytesSize = (int) (size / 8) * 3 + 3;
    dnaBytes = new byte[dnaBytesSize];
    createDnaBytes(stream);
  }
	
	private char getCharAt(int bytePos, int offset) {
		int x = ((int) dnaBytes[bytePos]) & 0xFF;  // bit gymnastics because of no unsigned in java.
		x <<= 8;
		x |= ((int) dnaBytes[bytePos + 1]) & 0xFF;
		x <<= 8;
		x |= ((int) dnaBytes[bytePos + 2]) & 0xFF;
		return decHashTable[(byte) ((x >> ((7 - offset) * 3)) & 0x07)];
	}
	
	public char charAt(long index) {
		int bytePos = (int) (index >> 3) * 3;
		return getCharAt(bytePos, (int) (index & 0x7));
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (long i = 0; i < dnaStringSize; ++i) {
			sb.append(charAt(i));
		}
		return sb.toString();
	}
	
	public long length() {
		return dnaStringSize;
	}

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Need file argument");
      return;
    }
    File file = new File(args[0]);
    BufferedInputStream stream = null;
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      stream = new BufferedInputStream(fis);

      // -1 to discount for newline character at the end.
      DNAString dnaString = new DNAString(stream, file.length() - 1);
      System.out.println(dnaString.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
