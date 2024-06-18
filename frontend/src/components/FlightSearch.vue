<template>
  <div class="container">
    <div class="flight-search">
      <h2>Flight Search</h2>
      <form @submit.prevent="searchFlights">

        <div class="date-picker">
          <label for="flightDates">Flight date(s)</label>
          <VueDatePicker v-model="flightDates" id="flightDates" :partial-range="true" :min-date="new Date()"
            prevent-min-max-navigation ignore-time-validation :enable-time-picker="false"
            :range="{ noDisabledRange: true }" :disabled-dates="disabledDates" />
        </div>

        <div class="checkbox">
          <input type="checkbox" v-model="directFlights" id="directFlights" />
          <label for="directFlights">Direct Flights Only</label>
        </div>

        <button type="submit">Search Flights</button>
      </form>
    </div>
    <div class="search-recap">
      <h2>Search Recap</h2>
      <div class="recap-item">
        <h4>Flight dates</h4>
        <p v-if="flightDates">{{ formatDates(flightDates) }} </p>
        <p v-if="flightDates && flightDates[1] !== null">{{ computeNDays(flightDates) }} / {{ computeNNights(flightDates) }}</p>
      </div>
      <div class="recap-item">
        <h4>Direct flight only?</h4>
        <p>{{ directFlights ? 'YES' : 'NO' }}</p>
      </div>
    </div>
  </div>
</template>

<script>
import VueDatePicker from '@vuepic/vue-datepicker';
import '@vuepic/vue-datepicker/dist/main.css';
import { ref, computed } from 'vue';

export default {
  components: {
    VueDatePicker,
  },
  setup() {
    const flightDates = ref(null);
    const directFlights = ref(false);

    const departureDate = computed(() => Array.isArray(flightDates.value) ? flightDates.value[0] : flightDates.value);
    const returnDate = computed(() => Array.isArray(flightDates.value) ? flightDates.value[1] : null);
    var isRoundtrip = computed(() => flightDates.value[1] !== null);

    const formatDates = (datesArray) => {
      console.log('datesArray:', datesArray);
      if (isRoundtrip.value) {
        return `${datesArray[0].toDateString()} - ${datesArray[1].toDateString()}`;
      } else {
        return datesArray[0].toDateString();
      }
    };

    const computeNDays = (datesArray) => {
      if (!Array.isArray(datesArray)) return '';
      const [startDate, endDate] = datesArray;
      const diffTime = Math.abs(endDate - startDate);
      return `${Math.ceil(diffTime / (1000 * 60 * 60 * 24))} days`;
    };

    const computeNNights = (datesArray) => {
      if (!Array.isArray(datesArray)) return '';
      const [startDate, endDate] = datesArray;
      const diffTime = Math.abs(endDate - startDate);
      return `${Math.ceil(diffTime / (1000 * 60 * 60 * 24)) - 1} nights`;
    };

    const searchFlights = () => {
      const searchParams = {
        departureDate: departureDate.value,
        returnDate: returnDate.value,
        directFlights: directFlights.value,
      };
      console.log('Searching flights with parameters:', searchParams);
      // Add your flight search logic here
    };

    return {
      flightDates,
      directFlights,
      formatDates,
      computeNDays,
      computeNNights,
      searchFlights,
    };
  },
};
</script>

<style>
@import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@300;400;600;800&display=swap');

.container {
  font-family: 'Open Sans', sans-serif;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
}

.flight-search,
.search-recap {
  width: 45%;
}

.flight-search {
  border-right: 1px solid #ccc;
  padding-right: 20px;
}

.flight-search label {
  display: block;
  margin-bottom: 5px;
  text-transform: uppercase;
}

.checkbox {
  margin-top: 15px;
}

button {
  margin-top: 20px;
  padding: 10px 15px;
  background-color: #007bff;
  color: white;
  border: none;
  border-radius: 5px;
  cursor: pointer;
}

button:hover {
  background-color: #0056b3;
}

.recap-item {
  margin-bottom: 20px;
}

.recap-item h4,
.recap-item p {
  margin: 0;
}
</style>
