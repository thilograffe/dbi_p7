package jdbcPostgres;

import java.util.Scanner;

public class ControlClass {
	final static String DIGITS="0123456789";

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		System.out.println("Herzlich willkommen zum Control-Panel des Datenbank-Systems der Gruppe dbi32!\n"
							+ "Möchten sie die Datenbank reseten oder befüllen? (Geben sie 0 für Ersteres oder 1 für letzteres ein!)");
		String str1 = scan.nextLine();
		if(stringtoint(str1)<=1&&stringtoint(str1)>=0) {
			if(stringtoint(str1)==0) {
				Start task=new Start(0,0,0,0,0);
				task.initialize();
			}
			else {
				System.out.println("Wie hoch soll der Skalierungsfaktor sein?)");
				String str2 = scan.nextLine();
				System.out.println("Wie groß soll die Batchgröße sein?)");
				String str3 = scan.nextLine();
				System.out.println("Wie viele Threads?)");
				String str4 = scan.nextLine();
				long startZeit = System.currentTimeMillis();
				Start task = new Start(1,stringtoint(str2),0,1,0);
				task.table(1);
				for(int i=0;i<stringtoint(str4);i++) {
					new Thread(new Start(1,stringtoint(str2),stringtoint(str3),stringtoint(str4),i)).start();
					System.out.println("Thread "+i+" ist gestartet.");
				}
				task.table(3);
				System.out.println("Allgemein: "+(System.currentTimeMillis() - startZeit));
				
			}
		}
		else {
			System.out.println("Das ist weder eine 0 noch eine 1! Tschüss!");
		}
		System.out.println("Ende");
		scan.close();
	}
	private static int stringtoint(String strin) {
		int j=0;
		for(int i=0;i<strin.length();i++) {
			j=j*10;
			j=j+DIGITS.indexOf(strin.charAt(i));
		}
		return j;
	}

}
