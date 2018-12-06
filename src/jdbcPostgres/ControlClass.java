package jdbcPostgres;

import java.util.Scanner;

public class ControlClass {
	private static int threadCount = 0;
	private static int callbackCount = 0;
	private static long time = 0;
	
	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		System.out.println("Herzlich willkommen zum Control-Panel des Datenbank-Systems der Gruppe dbi32!\n"
							+ "Möchten sie die Datenbank reseten oder befüllen? (Geben sie 0 für Ersteres oder 1 für letzteres ein!)");
		int mode = scan.nextInt();
		if(mode==1||mode==0) {
			if(mode==0) {
				new TaskClass().initialize();
			}
			else {
				new TaskClass().setTableTriggers(false);
				System.out.println("Wie hoch soll der Skalierungsfaktor sein?)");
				int n = (scan.nextInt());
				System.out.println("Wie groß soll die Batchgröße sein?)");
				int bs = (scan.nextInt());
				System.out.println("Wie viele Threads?)");
				threadCount = (scan.nextInt());
				TaskClass.configure(n,bs,threadCount);
				
				//Hier wird die Stoppuhr gestartet.
				System.out.println("Allgemeiner Start: "+(System.currentTimeMillis())); 
				time=System.currentTimeMillis();
				for(int i=0;i<threadCount;i++) {
					new Thread(new TaskClass(i)).start();
				}
				
			}
		}
		else {
			System.out.println("Das ist weder eine 0 noch eine 1!");
		}
		scan.close();
	}
	
	public static void callback(int threadIndex) {
		callbackCount++;
		if(callbackCount==threadCount) {
			//Hier wird die Stoppuhr gestoppt.
			System.out.println("Benchmark-Ergebnis: "+(System.currentTimeMillis()-time)); 
			System.out.println("Allgemeines Ende: "+(System.currentTimeMillis())); 
			new TaskClass().setTableTriggers(true);
		}
	}
}