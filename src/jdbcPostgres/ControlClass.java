package jdbcPostgres;

import java.util.Scanner;

public class ControlClass {

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		System.out.println("Herzlich willkommen zum Control-Panel des Datenbank-Systems der Gruppe dbi32!\n"
							+ "Möchten sie die Datenbank reseten oder befüllen? (Geben sie 0 für Ersteres oder 1 für letzteres ein!)");
		int mode = scan.nextInt();
		if(mode==1||mode==0) {
			if(mode==0) {
				Start task=new Start(0,0,0,0,0);
				task.initialize();
			}
			else {
				System.out.println("Wie hoch soll der Skalierungsfaktor sein?)");
				int n = (scan.nextInt());
				System.out.println("Wie groß soll die Batchgröße sein?)");
				int bs = (scan.nextInt());
				System.out.println("Wie viele Threads?)");
				int tc = (scan.nextInt());
				System.out.println("Allgemeiner Start: "+(System.currentTimeMillis())); 
				for(int i=0;i<tc;i++) {
					new Thread(new Start(1,n,bs,tc,i)).start();
				}
				
			}
		}
		else {
			System.out.println("Das ist weder eine 0 noch eine 1!");
		}
		scan.close();
	}
	

}
