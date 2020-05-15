package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type CandleNote struct {
	name         string
	minPrice     float64
	maxPrice     float64
	income       float64
	minPriceTime time.Time
	maxPriceTime time.Time
}

// Структура для пользовательских данных вида:
//		{id, {"ticker1": {"salePrice": float64,
//						 "buyPrice": float64,
//						 "income": float64}}, }
type UserNote struct {
	id      string
	tickers map[string]map[string]float64
}

func main() {
	candles, users, err := readFiles()
	checkErr(err)

	maxRevenueMap, err := ExtractCandles(candles)
	checkErr(err)

	userInfo := ExtractUsersTrans(users)

	result := FormatData(maxRevenueMap, userInfo)

	err = WriteCSVToFile(result, "output.csv")
	checkErr(err)
}

// Формирование словаря для содержания тикера, максимальной цены и времени,
// и минимальной цены и времени.
func ExtractCandles(candles [][]string) (map[string]CandleNote, error) {
	maxRevenueMap := make(map[string]CandleNote)

	for _, candle := range candles {
		t, err := time.Parse(time.RFC3339, candle[1])
		if err != nil {
			log.Printf("unable to parse candle[1]: %s\n", err)
			return nil, err
		}

		maxPrice, err := StringToFloat(candle[3])
		checkErr(err)

		minPrice, err := StringToFloat(candle[4])
		checkErr(err)

		if note, ok := maxRevenueMap[candle[0]]; ok {
			// Работа с обновлением данных
			if minPrice < note.minPrice {
				note.minPrice = minPrice
				note.minPriceTime = t
			}

			if maxPrice > note.maxPrice {
				note.maxPrice = maxPrice
				note.maxPriceTime = t
			}

			note.income = note.maxPrice - note.minPrice
			maxRevenueMap[candle[0]] = note
		} else {
			maxRevenueMap[candle[0]] = CandleNote{candle[0], minPrice, maxPrice, maxPrice - minPrice, t, t}
		}
	}

	return maxRevenueMap, nil
}

// Формирование словаря с информацией о выгоде пользовательских транзакций.
func ExtractUsersTrans(users [][]string) map[string]UserNote {
	usersInfo := make(map[string]UserNote)

	for _, user := range users {
		if note, ok := usersInfo[user[0]]; ok {
			if _, ok := note.tickers[user[2]]; ok {
				salePrice, err := StringToFloat(user[4])
				checkErr(err)
				note.tickers[user[2]]["salePrice"] = salePrice
				note.tickers[user[2]]["income"] = note.tickers[user[2]]["salePrice"] - note.tickers[user[2]]["buyPrice"]
			} else {
				buyPrice, err := StringToFloat(user[3])
				checkErr(err)
				note.tickers[user[2]] = map[string]float64{
					"buyPrice": buyPrice,
				}
			}
		} else {
			buyPrice, err := StringToFloat(user[3])
			checkErr(err)
			ticker := map[string]map[string]float64{
				user[2]: {
					"buyPrice": buyPrice,
				},
			}
			usersInfo[user[0]] = UserNote{user[0], ticker}
		}
	}

	return usersInfo
}

// Формирование выходного файла "output.csv" или объекта csv для записи в файл.
// Принимает на вход 2 словаря, являющихся резулятатами работы функций ExtractUsersTrans и ExtractCandles.
func FormatData(maxRevenueMap map[string]CandleNote, usersInfo map[string]UserNote) [][]string {
	var result [][]string

	for user := range usersInfo {
		for key, ticker := range usersInfo[user].tickers {
			var line []string
			//fmt.Print(type(ticker))
			userID := user
			userRevenue := ticker["income"]
			maxRevenue := maxRevenueMap[key].income
			diff := maxRevenue - userRevenue
			timeToSale := maxRevenueMap[key].maxPriceTime.Format("2006-01-02T15:04:05Z")
			timeToBuy := maxRevenueMap[key].minPriceTime.Format("2006-01-02T15:04:05Z")

			line = append(line, userID, key, fmt.Sprintf("%.2f", userRevenue), fmt.Sprintf("%.2f", maxRevenue), fmt.Sprintf("%.2f", diff), timeToSale, timeToBuy)
			result = append(result, line)
		}
	}

	return result
}

// Функция записи данных в файл.
func WriteCSVToFile(data [][]string, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		log.Printf("unable to create %s\n", fileName)
		return err
	}

	w := csv.NewWriter(file)

	err = w.WriteAll(data)
	if err != nil {
		log.Printf("unable to write in %s\n", fileName)
		return err
	}

	return err
}

// Чтение файлов.
func readFiles() ([][]string, [][]string, error) {
	candlesFile, err := os.Open("./candles_5m.csv")
	if err != nil {
		log.Printf("unable to open file ./candles_5m.csv\n")
		return nil, nil, err
	}

	usersFile, err := os.Open("./user_trades.csv")
	if err != nil {
		log.Printf("unable to open file ./user_trades.csv\n")
		return nil, nil, err
	}

	// Парсинг csv файлов.
	candlesReader := csv.NewReader(candlesFile)
	usersReader := csv.NewReader(usersFile)

	candles, err := candlesReader.ReadAll()
	if err != nil {
		log.Printf("unable to read var 'candlesReader'\n")
		return nil, nil, err
	}

	users, err := usersReader.ReadAll()
	if err != nil {
		log.Printf("unable to read var 'userReader'\n")
		return nil, nil, err
	}

	return candles, users, err
}

// Конвертирование строк в числа из пользовательского файла.
func StringToFloat(s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Printf("unable to convert %s to float\n", s)
		return f, err
	}

	return f, nil
}

// Функция обработки ошибок
func checkErr(err error) {
	if err != nil {
		log.Println(err)
	}
}
